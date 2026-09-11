# kafka-dedup-dotnet
Deduplicate a kafka topic into another topic

## Configuration
Is done via environment variables. Following options are available:

### Source Kafka Cluster (Consumer)
* KAFKA__BROKERS (or SOURCE_KAFKA__BROKERS)
* KAFKA__TLS__CA_LOCATION (or SOURCE_KAFKA__TLS__CA_LOCATION)
* KAFKA__TLS__CERTIFICATE_LOCATION (or SOURCE_KAFKA__TLS__CERTIFICATE_LOCATION)
* KAFKA__TLS__KEY_LOCATION (or SOURCE_KAFKA__TLS__KEY_LOCATION)
* KAFKA__USERNAME (or SOURCE_KAFKA__USERNAME)
* KAFKA__PASSWORD (or SOURCE_KAFKA__PASSWORD)
* SOURCE_KAFKA__AUTO_OFFSET_RESET (default: earliest, options: earliest/latest)
* SOURCE_TOPIC

### Target Kafka Cluster (Producer)
* TARGET_KAFKA__BROKERS (optional, defaults to source cluster)
* TARGET_KAFKA__TLS__CA_LOCATION (optional, defaults to source cluster)
* TARGET_KAFKA__TLS__CERTIFICATE_LOCATION (optional, defaults to source cluster)
* TARGET_KAFKA__TLS__KEY_LOCATION (optional, defaults to source cluster)
* TARGET_KAFKA__USERNAME (optional, defaults to source cluster)
* TARGET_KAFKA__PASSWORD (optional, defaults to source cluster)
* TARGET_TOPIC

### Redis & Processing
* REDIS_HOST  
* REDIS_PASSWORD  
* BATCH_SIZE (default 50)
* GROUP_ID (default: deduper)
* SOURCE_LOOKBACK_HOURS (optional) - when set, assigned consumer partitions start at the offset closest to `now - lookback` instead of the committed/stored offset.
* SOURCE_END_TIMESTAMP_UTC (optional) - when set, consumption stops before the first source record at or after the provided UTC timestamp.

### Backlog Recovery
The deduper will process and forward messages up to 24 hours old. Entire batches older than 24 hours are dropped and their offsets are committed.

### Forwarding
Messages may provide a UTF-8 `deduplication-key` header to identify duplicates independently of the Kafka key. Without it, the Kafka key remains the duplicate identity. Forwarding preserves the original key and headers, allowing sales of the same item to share a partition while distinct auction UUIDs remain distinct events. Deploy this deduper version before enabling producers that use this header.

* FORWARD_ONLY (optional, default: false) - when set to true the application will skip deduplication and forward every consumed message to the target topic. Useful for migration or simple forwarding use-cases.

**Note:** If `TARGET_KAFKA__*` variables are not set, the application will use the same cluster as the source (backwards compatible). The original `KAFKA__*` variables are still supported for backwards compatibility.

The project and Docker image target .NET 10.

### Sky Bazaar Replay Prep
To prepare a local replay of the last 24 hours from `sky-bazaar-raw` into `sky-bazaar`, run `./scripts/prepare-sky-bazaar-replay.sh`. It extracts the required Kafka credentials and TLS files from the live `kafka/dedup-sky-bazaar` deployment into `.replay/sky-bazaar/` and generates a replay env file with a unique consumer group and `SOURCE_LOOKBACK_HOURS=24`.

Then start the replay with `./scripts/run-sky-bazaar-replay.sh`. The script builds the current image, starts a temporary local Redis container, mounts the extracted TLS files, and runs the deduper against the prepared env file.

For other replay directories under `.replay/<name>/`, use `./scripts/run-topic-replay.sh <name>`. Set `SKIP_BUILD=true` to reuse an already built image across multiple sequential topic replays.

### Load test
When started with the argument `--test` `MESSAGE_COUNT` messages (default 100k) will be sent to the `SOURCE_TOPIC` topic. (upper case words are environment variables)
