# kafka-dedup-dotnet
Deduplicate a kafka topic into another topic

## Configuration
Is done via environment variables. Following options are available:
* KAFKA__BROKERS  
* KAFKA__TLS__CA_LOCATION
* KAFKA__TLS__CERTIFICATE_LOCATION
* KAFKA__TLS__KEY_LOCATION
* KAFKA__USERNAME
* KAFKA__PASSWORD 
* REDIS_HOST  
* REDIS_PASSWORD  
* SOURCE_TOPIC  
* TARGET_TOPIC  
* BATCH_SIZE  (default 50)  


### Load test
When started with the argument `--test` `MESSAGE_COUNT` messages (default 100k) will be sent to the `SOURCE_TOPIC` topic. (upper case words are environment variables)