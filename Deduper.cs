using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using SimplerConfig;
using Prometheus;
using StackExchange.Redis;

namespace Coflnet.Kafka.Dedup
{
    class Deduper
    {
        private readonly static Counter MessagesAcknowledged = Metrics.CreateCounter("deduper_acknowledged", "How many messages were acknowledged");
        private readonly static Counter OldMessagesDropped = Metrics.CreateCounter("deduper_dropped", "How many messages were dropped because to old");
        private readonly static Gauge CurrentOffset = Metrics.CreateGauge("dedup_consume_offset", "The consumer group offset");

        private string produceIntoTopic = SimplerConfig.Config.Instance["TARGET_TOPIC"];
        private string sourceTopic = SimplerConfig.Config.Instance["SOURCE_TOPIC"];
        private string GroupId = SimplerConfig.Config.Instance["GROUP_ID"] ?? "deduper";
        static int batchSize = 50;
        private static bool FatalError = false;

        private ConcurrentDictionary<string, DateTime> Seen = new ConcurrentDictionary<string, DateTime>();


        Action<DeliveryReport<string, Carrier>> handler = r =>
            {
                if (r.Error.IsError || r.TopicPartitionOffset.Offset % (batchSize * 10) == 0)
                    Console.WriteLine(!r.Error.IsError
                        ? $"Delivered {r.Topic} {r.Offset} "
                        : $"\nDelivery Error {r.Topic}: {r.Error.Reason} {r.Error.IsFatal}");
                if (r.Error.IsFatal)
                {    
                    Console.WriteLine("Fatal error " + r.Error.Reason);
                    FatalError = true;
                }
                MessagesAcknowledged.Inc();
            };

        public ConnectionMultiplexer RedisConnection { get; private set; }
        public async Task Run(CancellationToken stopToken)
        {
            Console.WriteLine("starting");
            if (int.TryParse(SimplerConfig.Config.Instance["BATCH_SIZE"], out int size))
                batchSize = size;

            ConfigurationOptions options = ConfigurationOptions.Parse(SimplerConfig.Config.Instance["REDIS_HOST"]);
            options.Password = SimplerConfig.Config.Instance["REDIS_PASSWORD"];
            //options.AsyncTimeout = 200;
            RedisConnection = ConnectionMultiplexer.Connect(options);

            var db = RedisConnection.GetDatabase();
            var config = (SConfig.Instance as SConfig).AppSettings;
            var consumerConfig = new ConsumerConfig(GetClientConfig(config.GetSection("KAFKA")))
            {
                GroupId = GroupId,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = false // everything is commited explicitly
            };
            var producerConfig = new ProducerConfig(GetClientConfig(config.GetSection("KAFKA")));
            using (var c = new ConsumerBuilder<string, Carrier>(consumerConfig).SetValueDeserializer(Deserializer.Instance).Build())
            {
                using (var p = new ProducerBuilder<string, Carrier>(producerConfig).SetValueSerializer(Serializer.Instance).Build())
                {
                    c.Subscribe(sourceTopic);
                    Console.WriteLine($"connected with {c.Name} {GroupId}");
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
            var batch = new List<ConsumeResult<string, Carrier>>();
            while (!stopToken.IsCancellationRequested)
            {
                if(FatalError)
                    throw new Exception("Fatal error in kafka, abording to preserve data");
                try
                {
                    var next = c.Consume(stopToken);
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

                    var targetBatch = batch;
                    batch = new List<ConsumeResult<string, Carrier>>();
                    CurrentOffset.Set(targetBatch.Last().TopicPartitionOffset.Offset);

                    RemoveDupplicates(db, c, p, targetBatch);
                    if (semaphore.CurrentCount == 0)
                        await Task.Delay(30);
                }
                catch (ConsumeException e)
                {
                    Console.WriteLine($"Error occured: {e.Error.Reason}\n{e.StackTrace}");
                }
            }
        }
        SemaphoreSlim semaphore = new SemaphoreSlim(20);
        SemaphoreSlim removalLock = new SemaphoreSlim(1);
        private void RemoveDupplicates(IDatabase db, IConsumer<string, Carrier> c, IProducer<string, Carrier> p, List<ConsumeResult<string, Carrier>> targetBatch)
        {
            _ = Task.Run(async () =>
            {
                for (int i = 0; i < 3; i++)
                    try
                    {
                        await semaphore.WaitAsync();
                        await ProcessBatch(db, c, p, targetBatch);
                        return;
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine($"failed to save {e.Message}\n{e.StackTrace}");
                    }
                    finally
                    {
                        semaphore.Release();
                    }
            }).ConfigureAwait(false);
        }

        private async Task ProcessBatch(IDatabase db, IConsumer<string, Carrier> c, IProducer<string, Carrier> p, List<ConsumeResult<string, Carrier>> targetBatch)
        {
            if (targetBatch.All(m => m.Message.Timestamp.UtcDateTime < DateTime.Now - TimeSpan.FromHours(14)))
            {
                a++;
                if (a % 1000 == 0)
                    Console.WriteLine("1k dropped");
                OldMessagesDropped.Inc(targetBatch.Count);
                // tell kafka that we processed this batch
                ResetBatch(c, targetBatch);
                return;
            }
            await DeduplicateAndProduce(db, c, p, targetBatch);


            if (Seen.Count > batchSize * 20)
            {
                if (removalLock.CurrentCount == 0)
                    return;
                await removalLock.WaitAsync();
                try
                {
                    RemoveOld();
                }
                finally
                {
                    removalLock.Release();
                }
            }
            return;
        }

        private void RemoveOld()
        {

            // everything older than a minute has to be in redis by now
            var toRemove = Seen.Where(s => s.Value < DateTime.Now - TimeSpan.FromSeconds(10)).Select(s => s.Key).ToList();
            foreach (var item in toRemove)
            {
                Seen.Remove(item, out DateTime _);
            }
            Console.WriteLine("Removed " + toRemove.Count);
        }

        private async Task DeduplicateAndProduce(IDatabase db, IConsumer<string, Carrier> c, IProducer<string, Carrier> p, List<ConsumeResult<string, Carrier>> batch)
        {
            var unduplicated = batch.Where(m => m.Message.Timestamp.UtcDateTime > (DateTime.Now - TimeSpan.FromHours(3)))
                                                .Where(m => Seen.TryAdd(m.Message.Key, DateTime.Now)).GroupBy(x => x.Message.Key).Select(y => y.First()).ToList();
            if (unduplicated.Count == 0)
            {
                if (batch.Count > 0 && lastKey != batch.First().Message.Key)
                {
                    lastKey = batch.First().Message.Key;
                    Console.WriteLine("skip because duplicate " + lastKey);
                }

                ResetBatch(c, batch);
                return;// no new keys
                //continue; 
            }
            var getWatch = Stopwatch.StartNew();
            var result = await db.StringGetAsync(unduplicated.Where(k => k.Message.Key != null).Select(s => new RedisKey(s.Message.Key)).ToArray()).ConfigureAwait(false);
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
                    _ = trans.StringSetAsync(value.Message.Key, "1", TimeSpan.FromSeconds(3600)).ConfigureAwait(false);
                }
            }


            var watch = Stopwatch.StartNew();
            await trans.ExecuteAsync().ConfigureAwait(false);
            if (unduplicated.Count % 4 == 1)
                Console.WriteLine($"Save took {watch.Elapsed}");

            // tell kafka that we stored the batch
            ResetBatch(c, batch);
        }

        public static AdminClientConfig GetClientConfig(IConfiguration config)
        {
            var baseConfig = new AdminClientConfig
            {
                BootstrapServers = config["BROKERS"],
                SslCaLocation = config["TLS:CA_LOCATION"],
                SslCertificateLocation = config["TLS:CERTIFICATE_LOCATION"],
                SslKeyLocation = config["TLS:KEY_LOCATION"],
                SaslUsername = config["USERNAME"],
                SaslPassword = config["PASSWORD"]
            };
            if (!string.IsNullOrEmpty(baseConfig.SaslUsername))
            {
                if (!string.IsNullOrEmpty(baseConfig.SslKeyLocation))
                    baseConfig.SecurityProtocol = SecurityProtocol.SaslSsl;
                else
                    baseConfig.SecurityProtocol = SecurityProtocol.SaslPlaintext;
                baseConfig.SaslMechanism = SaslMechanism.ScramSha256;
            }
            else
            {
                if (!string.IsNullOrEmpty(baseConfig.SslKeyLocation))
                    baseConfig.SecurityProtocol = SecurityProtocol.Ssl;
                else
                    baseConfig.SecurityProtocol = SecurityProtocol.Plaintext;
            }
            return baseConfig;
        }

        private static void ResetBatch(IConsumer<string, Carrier> c, List<ConsumeResult<string, Carrier>> batch)
        {
            c.Commit(batch.Select(b => b.TopicPartitionOffset).ToList());
            batch.Clear();
        }
    }
}