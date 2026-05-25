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

### Backlog Recovery
The deduper will process and forward messages up to 24 hours old. Entire batches older than 24 hours are dropped and their offsets are committed.

### Forwarding
* FORWARD_ONLY (optional, default: false) - when set to true the application will skip deduplication and forward every consumed message to the target topic. Useful for migration or simple forwarding use-cases.

**Note:** If `TARGET_KAFKA__*` variables are not set, the application will use the same cluster as the source (backwards compatible). The original `KAFKA__*` variables are still supported for backwards compatibility.

The project and Docker image target .NET 8.

### Load test
When started with the argument `--test` `MESSAGE_COUNT` messages (default 100k) will be sent to the `SOURCE_TOPIC` topic. (upper case words are environment variables)