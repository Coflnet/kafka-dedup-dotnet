using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Confluent.Kafka;
using StackExchange.Redis;

namespace Coflnet.Kafka.Dedup
{
    class Program
    {
        private static ProducerConfig producerConfig = new ProducerConfig { BootstrapServers = SimplerConfig.Config.Instance["KAFKA_HOST"] };
        private static ConsumerConfig consumerConfig = new ConsumerConfig
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
        private static string produceIntoTopic = SimplerConfig.Config.Instance["TARGET_TOPIC"];
        private static string sourceTopic = SimplerConfig.Config.Instance["SOURCE_TOPIC"];
        static int batchSize = 50;

        static Action<DeliveryReport<string, Carrier>> handler = r =>
            {
                if (r.Error.IsError || r.TopicPartitionOffset.Offset % (batchSize * 10) == 0)
                    Console.WriteLine(!r.Error.IsError
                        ? $"Delivered {r.Topic} {r.Offset} "
                        : $"\nDelivery Error {r.Topic}: {r.Error.Reason}");
            };

        public static ConnectionMultiplexer RedisConnection { get; private set; }

        static async Task Main(string[] args)
        {
            if (int.TryParse(SimplerConfig.Config.Instance["BATCH_SIZE"], out int size))
                batchSize = size;
            int msWaitTime = 10;
            if (int.TryParse(SimplerConfig.Config.Instance["BATCH_WAIT_TIME"], out int time))
                msWaitTime = time;

            var batch = new List<ConsumeResult<string, Carrier>>();
            ConfigurationOptions options = ConfigurationOptions.Parse(SimplerConfig.Config.Instance["REDIS_HOST"]);
            options.Password = SimplerConfig.Config.Instance["REDIS_PASSWORD"];
            options.AsyncTimeout = 200;
            RedisConnection = ConnectionMultiplexer.Connect(options);

            var db = RedisConnection.GetDatabase();
            Task<bool> lastSave = null;
            using (var c = new ConsumerBuilder<string, Carrier>(consumerConfig).SetValueDeserializer(Deserializer.Instance).Build())
            {
                using (var p = new ProducerBuilder<string, Carrier>(producerConfig).SetValueSerializer(Serializer.Instance).Build())
                {
                    c.Subscribe(sourceTopic);
                    try
                    {
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
                                if(lastSave != null)
                                    await lastSave;
                                    
                                var result = await db.StringGetAsync(unduplicated.Where(k => k.Message.Key != null).Select(s => new RedisKey(s.Message.Key)).ToArray());
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
                    catch (OperationCanceledException)
                    {
                        // Ensure the consumer leaves the group cleanly and final offsets are committed.
                        c.Close();
                    }
                }
            }
        }

        class Carrier
        {
            public byte[] content;
        }

        class Serializer : ISerializer<Carrier>
        {
            public static Serializer Instance = new Serializer();
            public byte[] Serialize(Carrier data, SerializationContext context)
            {
                return data.content;
            }
        }

        class Deserializer : IDeserializer<Carrier>
        {
            public static Deserializer Instance = new Deserializer();

            Carrier IDeserializer<Carrier>.Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
            {
                return new Carrier() { content = data.ToArray() };
            }
        }
    }
}