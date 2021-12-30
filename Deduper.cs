using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
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

        private ConcurrentDictionary<string, DateTime> Seen = new ConcurrentDictionary<string, DateTime>();


        Action<DeliveryReport<string, Carrier>> handler = r =>
            {
                if (r.Error.IsError || r.TopicPartitionOffset.Offset % (batchSize * 10) == 0)
                    Console.WriteLine(!r.Error.IsError
                        ? $"Delivered {r.Topic} {r.Offset} "
                        : $"\nDelivery Error {r.Topic}: {r.Error.Reason}");
            };

        public ConnectionMultiplexer RedisConnection { get; private set; }
        public async Task Run(CancellationToken stopToken)
        {
            Console.WriteLine("starting");
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
                    Console.WriteLine("connected");
                    try
                    {
                        await DedupBatch(db, c, p, stopToken);
                    }
                    catch (OperationCanceledException)
                    {
                    }
                    // Ensure the consumer leaves the group cleanly and final offsets are committed.
                    c.Close();
                    p.Flush();
                    Console.WriteLine("stopping");
                }
            }
        }

        private string lastKey;
        private int a = 0;

        public async Task DedupBatch(IDatabase db, IConsumer<string, Carrier> c, IProducer<string, Carrier> p, CancellationToken stopToken)
        {
            Task<bool> lastSave = null;
            var batch = new List<ConsumeResult<string, Carrier>>();
            while (!stopToken.IsCancellationRequested)
            {
                try
                {
                    var next = c.Consume(stopToken);
                    if (next.Message.Timestamp.UtcDateTime < DateTime.Now - TimeSpan.FromHours(14))
                    {
                        a++;
                        if (a % 1000000 == 0)
                            Console.WriteLine("1M");

                        c.Commit(new Confluent.Kafka.TopicPartitionOffset[] { next.TopicPartitionOffset });
                        continue;
                    }
                    if (next != null)
                        batch.Add(next);
                    while (batch.Count < batchSize)
                    {
                        var cr = c.Consume(TimeSpan.Zero);
                        if (cr == null)
                        {
                            break;
                        }

                        batch.Add(cr);
                        if (cr.TopicPartitionOffset.Offset % (batchSize * 10) == 0)
                            Console.WriteLine($"Consumed message '{cr.Message.Key}' at: '{cr.TopicPartitionOffset}'.");
                    }
                    if (batch.Count == 0)
                        continue;
                    // remove dupplicates
                    var unduplicated = batch.Where(m => m.Message.Timestamp.UtcDateTime > (DateTime.Now - TimeSpan.FromHours(3))).GroupBy(x => x.Message.Key).Select(y => y.First()).ToList();
                    if (unduplicated.Count == 0)
                    {
                        if (lastKey != batch.First().Message.Key)
                        {
                            lastKey = batch.First().Message.Key;
                            Console.WriteLine("skip because duplicate " + lastKey);
                        }

                        ResetBatch(c, batch);
                        continue; // no new keys
                    }

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
                    ResetBatch(c, batch);
                }
                catch (ConsumeException e)
                {
                    Console.WriteLine($"Error occured: {e.Error.Reason}");
                }
            }
        }

        private static void ResetBatch(IConsumer<string, Carrier> c, List<ConsumeResult<string, Carrier>> batch)
        {
            c.Commit(batch.Select(b => b.TopicPartitionOffset));
            batch.Clear();
        }
    }
}