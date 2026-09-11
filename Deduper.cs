using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
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
        private readonly static Counter MessagesError = Metrics.CreateCounter("deduper_error", "How many messages failed to be acknowledged");
        private readonly static Counter OldMessagesDropped = Metrics.CreateCounter("deduper_dropped", "How many messages were dropped because to old");
        private readonly static Counter MessagesSkipped = Metrics.CreateCounter("deduper_skipped", "How many messages were skipped because they could not be decoded (corrupt/undecompressable batch)");
        private readonly static Gauge CurrentOffset = Metrics.CreateGauge("dedup_consume_offset", "The consumer group offset");
        private static readonly TimeSpan ProcessingWindow = TimeSpan.FromHours(24);
        private static readonly TimeSpan LookbackOffsetsTimeout = TimeSpan.FromSeconds(10);

        private string produceIntoTopic = GetSetting("TARGET_TOPIC");
        private string sourceTopic = GetSetting("SOURCE_TOPIC");
        private string GroupId = GetSetting("GROUP_ID") ?? "deduper";
        static int batchSize = 50;
        private static bool FatalError = false;
        private DateTime? replayEndTimestampUtc;

        private ConcurrentDictionary<string, DateTime> Seen = new ConcurrentDictionary<string, DateTime>();
        private ConcurrentDictionary<TopicPartition, Offset> ReplayEndOffsets = new ConcurrentDictionary<TopicPartition, Offset>();
        private ConcurrentDictionary<TopicPartition, byte> CompletedReplayPartitions = new ConcurrentDictionary<TopicPartition, byte>();
        private List<Task> InFlightBatches = new List<Task>();
        private bool ForwardOnly = false;


        Action<DeliveryReport<string, Carrier>> handler = r =>
            {
                if (r.Error.IsError || r.TopicPartitionOffset.Offset % (batchSize * 10) == 0)
                    Console.WriteLine(!r.Error.IsError
                        ? $"Delivered {r.Topic} {r.Offset} "
                        : $"\nDelivery Error {r.Topic}: {r.Error.Reason} {r.Error.IsFatal}");
                if (r.Error.IsFatal || r.Error.Reason.Contains("Message timed out"))
                {
                    Console.WriteLine("Fatal error " + r.Error.Reason);
                    FatalError = true;
                }
                if (!r.Error.IsError)
                    MessagesAcknowledged.Inc();
                else
                    MessagesError.Inc();
            };

        public ConnectionMultiplexer RedisConnection { get; private set; }
        public async Task Run(CancellationToken stopToken)
        {
            Console.WriteLine("starting");
            if (int.TryParse(GetSetting("BATCH_SIZE"), out int size))
                batchSize = size;

            ConfigurationOptions options = ConfigurationOptions.Parse(GetRequiredSetting("REDIS_HOST"));
            options.Password = GetSetting("REDIS_PASSWORD");
            //options.AsyncTimeout = 200;
            RedisConnection = ConnectionMultiplexer.Connect(options);

            var db = RedisConnection.GetDatabase();
            var appSettings = (SConfig.Instance as SConfig)?.AppSettings;
            var configBuilder = new ConfigurationBuilder();
            if (appSettings != null)
                configBuilder.AddConfiguration(appSettings);

            configBuilder.AddEnvironmentVariables();
            var config = configBuilder.Build();
            
            // Support both SOURCE_KAFKA and KAFKA (backwards compatible)
            var sourceKafkaSection = config.GetSection("SOURCE_KAFKA");
            if (!sourceKafkaSection.Exists() || string.IsNullOrWhiteSpace(sourceKafkaSection["BROKERS"]))
                sourceKafkaSection = config.GetSection("KAFKA");
            
            // Target Kafka can be different cluster or same as source
            var targetKafkaSection = config.GetSection("TARGET_KAFKA");
            if (!targetKafkaSection.Exists() || string.IsNullOrWhiteSpace(targetKafkaSection["BROKERS"]))
                targetKafkaSection = sourceKafkaSection;
            
            // Read auto offset reset setting (default to Earliest for backwards compatibility)
            var autoOffsetResetValue = GetSetting("SOURCE_KAFKA__AUTO_OFFSET_RESET") 
                ?? GetSetting("AUTO_OFFSET_RESET") 
                ?? "earliest";
            var autoOffsetReset = autoOffsetResetValue.ToLower() == "latest" 
                ? AutoOffsetReset.Latest 
                : AutoOffsetReset.Earliest;
            
            var consumerConfig = new ConsumerConfig(GetClientConfig(sourceKafkaSection))
            {
                GroupId = GroupId,
                AutoOffsetReset = autoOffsetReset,
                SessionTimeoutMs = 8000,
                SocketTimeoutMs = 8000,
                EnableAutoCommit = false // everything is commited explicitly
            };
            var producerConfig = new ProducerConfig(GetClientConfig(targetKafkaSection));

            // Option to disable deduplication and forward messages as-is
            var forwardOnlyValue = GetSetting("FORWARD_ONLY") ?? GetSetting("DISABLE_DEDUP") ?? "false";
            ForwardOnly = forwardOnlyValue.Equals("true", StringComparison.OrdinalIgnoreCase) || forwardOnlyValue == "1";
            var consumerBuilder = new ConsumerBuilder<string, Carrier>(consumerConfig)
                .SetValueDeserializer(Deserializer.Instance);
            var hasSourceLookback = TryGetSourceLookback(out var sourceLookback);
            var hasReplayEndTimestamp = TryGetSourceEndTimestamp(out var configuredReplayEndTimestampUtc);
            replayEndTimestampUtc = hasReplayEndTimestamp ? configuredReplayEndTimestampUtc : null;

            if (hasSourceLookback && hasReplayEndTimestamp)
            {
                consumerBuilder.SetPartitionsAssignedHandler((consumer, partitions) => ResolveReplayAssignments(consumer, partitions, sourceLookback, configuredReplayEndTimestampUtc));
            }
            else if (hasSourceLookback)
            {
                consumerBuilder.SetPartitionsAssignedHandler((consumer, partitions) => ResolveReplayOffsets(consumer, partitions, sourceLookback));
            }
            else if (hasReplayEndTimestamp)
            {
                consumerBuilder.SetPartitionsAssignedHandler((consumer, partitions) => ConfigureReplayEndOffsets(consumer, partitions, configuredReplayEndTimestampUtc));
            }

            using (var c = consumerBuilder.Build())
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
                    if (replayEndTimestampUtc.HasValue && HasReachedReplayEnd())
                        Console.WriteLine("Replay cutoff reached. Skipping consumer close because offsets were already committed during batch processing.");
                    else
                    {
                        Console.WriteLine("Closing consumer.");
                        c.Close();
                    }

                    Console.WriteLine("Flushing producer.");
                    var remainingMessages = p.Flush(TimeSpan.FromSeconds(15));
                    Console.WriteLine($"Producer flush complete. Remaining messages: {remainingMessages}.");
                    Console.WriteLine("stopping");
                }
            }
        }

        private static bool TryGetSourceLookback(out TimeSpan sourceLookback)
        {
            sourceLookback = default;
            var configuredValue = GetSetting("SOURCE_LOOKBACK_HOURS")
                ?? GetSetting("REPLAY_LOOKBACK_HOURS");

            if (string.IsNullOrWhiteSpace(configuredValue))
                return false;

            if (!double.TryParse(configuredValue, out var lookbackHours) || lookbackHours <= 0)
                throw new InvalidOperationException($"Invalid SOURCE_LOOKBACK_HOURS value '{configuredValue}'. Expected a positive number of hours.");

            sourceLookback = TimeSpan.FromHours(lookbackHours);
            Console.WriteLine($"Starting source consumption {sourceLookback.TotalHours:0.##} hours back.");
            return true;
        }

        private static bool TryGetSourceEndTimestamp(out DateTime sourceEndTimestampUtc)
        {
            sourceEndTimestampUtc = default;
            var configuredValue = GetSetting("SOURCE_END_TIMESTAMP_UTC")
                ?? GetSetting("REPLAY_END_TIMESTAMP_UTC");

            if (string.IsNullOrWhiteSpace(configuredValue))
                return false;

            if (!DateTimeOffset.TryParse(configuredValue, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal, out var parsedValue))
                throw new InvalidOperationException($"Invalid SOURCE_END_TIMESTAMP_UTC value '{configuredValue}'. Expected an ISO-8601 UTC timestamp.");

            sourceEndTimestampUtc = parsedValue.UtcDateTime;
            Console.WriteLine($"Stopping source consumption at {sourceEndTimestampUtc:o}.");
            return true;
        }

        private static string GetRequiredSetting(string key)
        {
            var value = GetSetting(key);
            if (string.IsNullOrWhiteSpace(value))
                throw new InvalidOperationException($"Missing required configuration value '{key}'.");

            return value;
        }

        private static string GetSetting(string key)
        {
            try
            {
                var simplerConfigValue = SimplerConfig.Config.Instance[key];
                if (!string.IsNullOrWhiteSpace(simplerConfigValue))
                    return simplerConfigValue;
            }
            catch
            {
            }

            return Environment.GetEnvironmentVariable(key);
        }

        private static IEnumerable<TopicPartitionOffset> ResolveReplayOffsets(IConsumer<string, Carrier> consumer, List<TopicPartition> partitions, TimeSpan sourceLookback)
        {
            var replayStart = DateTime.UtcNow - sourceLookback;
            var requestedOffsets = partitions
                .Select(partition => new TopicPartitionTimestamp(partition, new Timestamp(replayStart)))
                .ToList();
            var resolvedOffsets = consumer.OffsetsForTimes(requestedOffsets, LookbackOffsetsTimeout);

            Console.WriteLine($"Assigned {partitions.Count} partitions from {replayStart:o}.");

            return resolvedOffsets.Select(offset =>
            {
                if (offset.Offset == Offset.Unset)
                    return new TopicPartitionOffset(offset.TopicPartition, Offset.Beginning);

                return offset;
            });
        }

        private IEnumerable<TopicPartitionOffset> ResolveReplayAssignments(IConsumer<string, Carrier> consumer, List<TopicPartition> partitions, TimeSpan sourceLookback, DateTime sourceEndTimestampUtc)
        {
            ConfigureReplayEndOffsets(consumer, partitions, sourceEndTimestampUtc);
            return ResolveReplayOffsets(consumer, partitions, sourceLookback);
        }

        private void ConfigureReplayEndOffsets(IConsumer<string, Carrier> consumer, List<TopicPartition> partitions, DateTime sourceEndTimestampUtc)
        {
            ReplayEndOffsets.Clear();
            CompletedReplayPartitions.Clear();

            var requestedOffsets = partitions
                .Select(partition => new TopicPartitionTimestamp(partition, new Timestamp(sourceEndTimestampUtc)))
                .ToList();
            var resolvedOffsets = consumer.OffsetsForTimes(requestedOffsets, LookbackOffsetsTimeout);

            foreach (var resolvedOffset in resolvedOffsets)
            {
                var replayEndOffset = resolvedOffset.Offset;
                if (replayEndOffset == Offset.Unset)
                    replayEndOffset = consumer.QueryWatermarkOffsets(resolvedOffset.TopicPartition, LookbackOffsetsTimeout).High;

                ReplayEndOffsets[resolvedOffset.TopicPartition] = replayEndOffset;
            }

            Console.WriteLine($"Will stop {partitions.Count} partitions at {sourceEndTimestampUtc:o}.");
        }

        private string lastKey;
        private int a = 0;

        public async Task DedupBatch(IDatabase db, IConsumer<string, Carrier> c, IProducer<string, Carrier> p, CancellationToken stopToken)
        {
            var batch = new List<ConsumeResult<string, Carrier>>();
            while (!stopToken.IsCancellationRequested)
            {
                if (FatalError)
                    throw new Exception("Fatal error in kafka, abording to preserve data");
                try
                {
                    if (HasReachedReplayEnd())
                        break;

                    TryCompleteReplayPartitions(c);
                    if (HasReachedReplayEnd())
                        break;

                    var next = c.Consume(TimeSpan.FromSeconds(1));
                    if (stopToken.IsCancellationRequested)
                        break;

                    TryAddToBatch(c, batch, next);
                    while (batch.Count < batchSize && !HasReachedReplayEnd())
                    {
                        var cr = c.Consume(TimeSpan.Zero);
                        if (cr == null)
                        {
                            break;
                        }

                        var addedToBatch = TryAddToBatch(c, batch, cr);
                        if (addedToBatch && cr.TopicPartitionOffset.Offset % (batchSize * 10) == 0)
                            Console.WriteLine($"Consumed message '{cr.Message.Key}' at: '{cr.TopicPartitionOffset}'.");
                    }
                    if (batch.Count == 0)
                    {
                        if (HasReachedReplayEnd())
                            break;

                        TryCompleteReplayPartitions(c);
                        if (HasReachedReplayEnd())
                            break;

                        continue;
                    }

                    var targetBatch = batch;
                    batch = new List<ConsumeResult<string, Carrier>>();
                    CurrentOffset.Set(targetBatch.Last().TopicPartitionOffset.Offset);

                    TrackInFlightBatch(RemoveDupplicates(db, c, p, targetBatch));
                    if (semaphore.CurrentCount == 0)
                        await Task.Delay(30);
                }
                catch (ConsumeException e)
                {
                    Console.WriteLine($"Error occured: {e.Error.Reason}\n{e.StackTrace}");

                    // A corrupt/undecompressable batch (BadCompression/BadMsg) never advances the
                    // consumer position: the next Consume() returns the exact same batch forever and
                    // the partition is wedged (this is the "Decompression (codec 0x3) ... Invalid
                    // compressed data" loop). Skip past it so the rest of the partition can flow.
                    if (e.Error.Code == ErrorCode.Local_BadCompression || e.Error.Code == ErrorCode.Local_BadMsg)
                    {
                        MessagesSkipped.Inc();
                        var failed = e.ConsumerRecord?.TopicPartitionOffset;
                        if (failed != null)
                        {
                            var resumeAt = new TopicPartitionOffset(failed.TopicPartition, failed.Offset + 1);
                            Console.WriteLine($"Skipping undecodable message at {failed}, seeking to {resumeAt.Offset}");
                            c.Seek(resumeAt);
                        }
                        else
                        {
                            // No offset on the exception — we cannot seek precisely. The pause below
                            // still prevents a tight loop; the partition stays put so lag keeps rising.
                            Console.WriteLine("Undecodable message without an offset on the exception; cannot seek, pausing before retry");
                        }

                        // Deliberate throttle: one skip per minute. A single bad batch costs a minute
                        // of latency on that partition (negligible). But if we start skipping *many*
                        // messages — i.e. actually losing data rather than one poison batch — throughput
                        // collapses and consumer lag climbs visibly instead of the deduper silently
                        // burning through the log, so the loss is alertable.
                        try { await Task.Delay(TimeSpan.FromMinutes(1), stopToken); }
                        catch (OperationCanceledException) { }
                    }
                }
            }

            if (HasReachedReplayEnd() && replayEndTimestampUtc.HasValue)
                Console.WriteLine($"Reached replay cutoff {replayEndTimestampUtc.Value:o}. Waiting for in-flight batches to finish.");

            await WaitForInFlightBatches();
            Console.WriteLine("Finished waiting for in-flight batches.");
        }
        SemaphoreSlim semaphore = new SemaphoreSlim(20);
        SemaphoreSlim removalLock = new SemaphoreSlim(1);
        SemaphoreSlim commitLock = new SemaphoreSlim(1);
        private Task RemoveDupplicates(IDatabase db, IConsumer<string, Carrier> c, IProducer<string, Carrier> p, List<ConsumeResult<string, Carrier>> targetBatch)
        {
            return Task.Run(async () =>
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
            });
        }

        private bool TryAddToBatch(IConsumer<string, Carrier> consumer, List<ConsumeResult<string, Carrier>> batch, ConsumeResult<string, Carrier> consumeResult)
        {
            if (consumeResult == null)
                return false;

            if (!ShouldProcessConsumeResult(consumer, consumeResult))
                return false;

            batch.Add(consumeResult);
            return true;
        }

        private bool ShouldProcessConsumeResult(IConsumer<string, Carrier> consumer, ConsumeResult<string, Carrier> consumeResult)
        {
            if (!ReplayEndOffsets.TryGetValue(consumeResult.TopicPartition, out var replayEndOffset))
                return true;

            if (consumeResult.Offset < replayEndOffset)
                return true;

            MarkReplayPartitionComplete(consumer, consumeResult.TopicPartition, consumeResult.Offset, replayEndOffset);

            return false;
        }

        private void TryCompleteReplayPartitions(IConsumer<string, Carrier> consumer)
        {
            if (ReplayEndOffsets.Count == 0)
                return;

            var assignedPartitions = consumer.Assignment;
            if (assignedPartitions == null || assignedPartitions.Count == 0)
                return;

            foreach (var topicPartition in assignedPartitions)
            {
                var position = new TopicPartitionOffset(topicPartition, consumer.Position(topicPartition));
                if (!ReplayEndOffsets.TryGetValue(position.TopicPartition, out var replayEndOffset))
                    continue;

                if (position.Offset == Offset.Unset || position.Offset < replayEndOffset)
                    continue;

                MarkReplayPartitionComplete(consumer, position.TopicPartition, position.Offset, replayEndOffset);
            }
        }

        private void MarkReplayPartitionComplete(IConsumer<string, Carrier> consumer, TopicPartition topicPartition, Offset currentOffset, Offset replayEndOffset)
        {
            if (!CompletedReplayPartitions.TryAdd(topicPartition, 0))
                return;

            consumer.Pause(new[] { topicPartition });
            Console.WriteLine($"Reached replay end for {topicPartition} at position {currentOffset} (target {replayEndOffset}).");
        }

        private bool HasReachedReplayEnd()
        {
            return ReplayEndOffsets.Count > 0 && CompletedReplayPartitions.Count == ReplayEndOffsets.Count;
        }

        private void TrackInFlightBatch(Task batchTask)
        {
            InFlightBatches.RemoveAll(task => task.IsCompleted);
            InFlightBatches.Add(batchTask);
        }

        private async Task WaitForInFlightBatches()
        {
            InFlightBatches.RemoveAll(task => task.IsCompleted);
            if (InFlightBatches.Count == 0)
                return;

            await Task.WhenAll(InFlightBatches);
            InFlightBatches.Clear();
        }

        private async Task ProcessBatch(IDatabase db, IConsumer<string, Carrier> c, IProducer<string, Carrier> p, List<ConsumeResult<string, Carrier>> targetBatch)
        {
            if (targetBatch.All(m => m.Message.Timestamp.UtcDateTime < DateTime.UtcNow - ProcessingWindow))
            {
                a++;
                if (a % 1000 == 0)
                    Console.WriteLine("1k dropped");
                OldMessagesDropped.Inc(targetBatch.Count);
                // tell kafka that we processed this batch
                await CommitBatch(c, targetBatch);
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

            // everything older than 10 sec has to be in redis by now
            var toRemove = Seen.Where(s => s.Value < DateTime.Now - TimeSpan.FromSeconds(10)).Select(s => s.Key).ToList();
            foreach (var item in toRemove)
            {
                Seen.Remove(item, out DateTime _);
            }
            Console.WriteLine("Removed " + toRemove.Count);
        }

        internal static string GetDeduplicationKey(Message<string, Carrier> message)
        {
            if (message.Headers != null && message.Headers.TryGetLastBytes("deduplication-key", out var key) && key != null)
                return System.Text.Encoding.UTF8.GetString(key);
            return message.Key;
        }

        private async Task DeduplicateAndProduce(IDatabase db, IConsumer<string, Carrier> c, IProducer<string, Carrier> p, List<ConsumeResult<string, Carrier>> batch)
        {
            if (ForwardOnly)
            {
                // Forward every message directly to target topic without deduplication/storage
                var ackBefore = MessagesAcknowledged.Value;
                foreach (var item in batch)
                {
                    p.Produce(produceIntoTopic, item.Message, handler);
                }

                // Wait shortly for at least one ack (best-effort)
                for (int i = 0; i < 50; i++)
                {
                    if (MessagesAcknowledged.Value > ackBefore)
                        break;
                    await Task.Delay(30);
                }

                // commit consumed offsets
                await CommitBatch(c, batch, ackBefore);
                return;
            }

            var unduplicated = batch.Where(m => m.Message.Timestamp.UtcDateTime > (DateTime.UtcNow - ProcessingWindow))
                                                .Where(m => Seen.TryAdd(GetDeduplicationKey(m.Message), DateTime.Now)).GroupBy(x => GetDeduplicationKey(x.Message)).Select(y => y.First()).ToList();
            if (unduplicated.Count == 0)
            {
                if (batch.Count > 0 && lastKey != batch.First().Message.Key)
                {
                    lastKey = batch.First().Message.Key;
                    Console.WriteLine("skip because duplicate " + lastKey);
                }

                await CommitBatch(c, batch);
                return;// no new keys
                //continue; 
            }
            var getWatch = Stopwatch.StartNew();
            var result = await db.StringGetAsync(unduplicated.Where(k => GetDeduplicationKey(k.Message) != null).Select(s => new RedisKey(GetDeduplicationKey(s.Message))).ToArray()).ConfigureAwait(false);
            if (result.Length % 2 == 1)
                Console.WriteLine($"Get {result.Length} took {getWatch.Elapsed} {DateTime.Now}");

            var acknowledgedCurrent = MessagesAcknowledged.Value;
            foreach (var item in unduplicated.Where(k => GetDeduplicationKey(k.Message) == null))
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
                    _ = trans.StringSetAsync(GetDeduplicationKey(value.Message), "1", TimeSpan.FromSeconds(3600)).ConfigureAwait(false);
                }
            }

            for (int i = 0; i < 50; i++)
            {
                if (MessagesAcknowledged.Value > acknowledgedCurrent)
                    break;
                await Task.Delay(30);
                if (i == 99)
                {
                    Console.WriteLine("timeout waiting for ack, aborting");
                    return;
                }
            }

            var watch = Stopwatch.StartNew();
            await trans.ExecuteAsync().ConfigureAwait(false);
            if (unduplicated.Count % 4 == 1)
                Console.WriteLine($"Save took {watch.Elapsed}");

            // tell kafka that we stored the batch
            await CommitBatch(c, batch, acknowledgedCurrent);
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

        private static void ResetBatch(IConsumer<string, Carrier> c, List<ConsumeResult<string, Carrier>> batch, double acknowledgedCurrent = -1)
        {
            if (acknowledgedCurrent != -1 && MessagesAcknowledged.Value <= acknowledgedCurrent)
                return; // did not get acknowledged by broker don't commit
            c.Commit(batch.Select(b => b.TopicPartitionOffset).ToList());
            batch.Clear();
        }

        private async Task CommitBatch(IConsumer<string, Carrier> c, List<ConsumeResult<string, Carrier>> batch, double acknowledgedCurrent = -1)
        {
            await commitLock.WaitAsync();
            try
            {
                ResetBatch(c, batch, acknowledgedCurrent);
            }
            finally
            {
                commitLock.Release();
            }
        }
    }
}
