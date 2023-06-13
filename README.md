# kafka-pedestal

Kafka consumer library to create applications using pedestal architecture and [stuartsierra Component](https://github.com/stuartsierra/component).

![Kafka Pedestal](https://github.com/georgehg/kafka-pedestal/assets/19939283/9bbee1fb-7a3d-408e-bbf8-7aea7671eee8)

It was freely based on the pedestal library io.pedestal.http and on the [pedestal.kafka](https://github.com/cognitect-labs/pedestal.kafka) github repostiory. From the former, we got the code design and structure to establish an interceptors provider chain for a kafka consumer application, from the latter, we got insights about the kafka consumption flow control.

The library uses the Manual Offset Control strategy as described in [kafka API documentation](https://kafka.apache.org/25/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html).
