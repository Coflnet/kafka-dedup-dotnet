# kafka-dedup-dotnet
Deduplicate a kafka topic into another topic

## Configuration
KAFKA_HOST  
REDIS_HOST  
REDIS_PASSWORD  
SOURCE_TOPIC  
TARGET_TOPIC  
BATCH_SIZE  (default 50)  
~~BATCH_WAIT_TIME~~ Deprecated


### Load test
When started with the argument `--test` `MESSAGE_COUNT` messages (default 100k) will be sent to the `SOURCE_TOPIC` topic. (upper case words are environment variables)