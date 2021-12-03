using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Confluent.Kafka;
using StackExchange.Redis;

namespace Coflnet.Kafka.Dedup
{
    class Deduper
    {
        private ProducerConfig producerConfig = new ProducerConfig { BootstrapServers = SimplerConfig.Config.Instance["KAFKA_HOST"] };
        private ConsumerConfig consumerConfig = new ConsumerConfig
        {
            GroupId = "test-consumer-group",
            BootstrapServers = SimplerConfig.Config.Instance["KAFKA_HOST"],
            // Note: The AutoOffsetReset property determines the start offset in the event
            // there are not yet any committed offsets for the consumer group for the
            // topic/partitions of interest. By default, offsets are committed
            // automatically, so in this example, consumption will only start from the
            // earliest message in the topic 'my-topic' the first time you run the program.
            AutoOffsetReset = AutoOffsetReset.Earliest
        };
        private string produceIntoTopic = SimplerConfig.Config.Instance["TARGET_TOPIC"];
        private string sourceTopic = SimplerConfig.Config.Instance["SOURCE_TOPIC"];
        static int batchSize = 50;
        int msWaitTime = 10;

        private Dictionary<string, short> Seen = new Dictionary<string, short>();


        Action<DeliveryReport<string, Carrier>> handler = r =>
            {
                if (r.Error.IsError || r.TopicPartitionOffset.Offset % (batchSize * 10) == 0)
                    Console.WriteLine(!r.Error.IsError
                        ? $"Delivered {r.Topic} {r.Offset} "
                        : $"\nDelivery Error {r.Topic}: {r.Error.Reason}");
            };

        public ConnectionMultiplexer RedisConnection { get; private set; }
        public async Task Run()
        {
            if (int.TryParse(SimplerConfig.Config.Instance["BATCH_SIZE"], out int size))
                batchSize = size;
            if (int.TryParse(SimplerConfig.Config.Instance["BATCH_WAIT_TIME"], out int time))
                msWaitTime = time;

            ConfigurationOptions options = ConfigurationOptions.Parse(SimplerConfig.Config.Instance["REDIS_HOST"]);
            options.Password = SimplerConfig.Config.Instance["REDIS_PASSWORD"];
            //options.AsyncTimeout = 200;
            RedisConnection = ConnectionMultiplexer.Connect(options);

            var db = RedisConnection.GetDatabase();
            using (var c = new ConsumerBuilder<string, Carrier>(consumerConfig).SetValueDeserializer(Deserializer.Instance).Build())
            {
                using (var p = new ProducerBuilder<string, Carrier>(producerConfig).SetValueSerializer(Serializer.Instance).Build())
                {
                    c.Subscribe(sourceTopic);
                    try
                    {
                        await DedupBatch(db, c, p);
                    }
                    catch (OperationCanceledException)
                    {
                        // Ensure the consumer leaves the group cleanly and final offsets are committed.
                        c.Close();
                    }
                }
            }
        }

        public async Task DedupBatch(IDatabase db, IConsumer<string, Carrier> c, IProducer<string, Carrier> p)
        {
            Task<bool> lastSave = null;
            var batch = new List<ConsumeResult<string, Carrier>>();
            while (true)
            {
                try
                {
                    while (batch.Count < batchSize)
                    {
                        var cr = c.Consume(msWaitTime);
                        if (cr == null)
                            break;

                        batch.Add(cr);
                        if (cr.TopicPartitionOffset.Offset % (batchSize * 10) == 0)
                            Console.WriteLine($"Consumed message '{cr.Message.Key}' at: '{cr.TopicPartitionOffset}'.");
                    }
                    if (batch.Count == 0)
                        continue;
                    // remove dupplicates
                    var unduplicated = batch.GroupBy(x => x.Message.Key).Select(y => y.First()).ToList();
                    if (lastSave != null)
                    {
                        var watch = Stopwatch.StartNew();
                        await lastSave;
                        if (unduplicated.Count % 4 == 1)
                            Console.WriteLine($"Save took {watch.Elapsed}");
                    }

                    var getWatch = Stopwatch.StartNew();
                    var result = await db.StringGetAsync(unduplicated.Where(k => k.Message.Key != null).Select(s => new RedisKey(s.Message.Key)).ToArray());
                    if (result.Length % 2 == 1)
                        Console.WriteLine($"Get {result.Length} took {getWatch.Elapsed} {DateTime.Now}");

                    foreach (var item in unduplicated.Where(k => k.Message.Key == null))
                    {
                        p.Produce(produceIntoTopic, item.Message, handler);
                    }

                    var trans = db.CreateTransaction();
                    for (int i = 0; i < unduplicated.Count; i++)
                    {
                        if (!result[i].HasValue)
                        {
                            var value = unduplicated[i];
                            p.Produce(produceIntoTopic, value.Message, handler);
                            trans.StringSetAsync(value.Message.Key, "1", TimeSpan.FromSeconds(3600));
                        }
                    }

                    lastSave = trans.ExecuteAsync();

                    // tell kafka that we stored the batch
                    c.Commit(batch.Select(b => b.TopicPartitionOffset));
                    batch.Clear();
                }
                catch (ConsumeException e)
                {
                    Console.WriteLine($"Error occured: {e.Error.Reason}");
                }
            }
        }
    }
}