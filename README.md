# kafka-vanilla-spring-boot-demo

The main motivation of this project is to showcase how to integrate Kafka vanilla client in Spring Boot application.

Some ideas you might find in the project :
* Dynamic loading of Producer/Consumer properties
* Producer/Consumer metrics collection via Micrometer/Prometheus
* Support multiple consumer threads
* Deserialization & Processing Error Handling (has various strategies including dead letter queue)
* Using Avro Generated classes

## Start the environment
To start the environment simply run the following command
```bash
docker-compose up -d
```
This would start a local Kafka cluster (single node) and UI ([Confluent Control Center](https://docs.confluent.io/platform/current/control-center/index.html)).

Once started you can run the application by running 
```bash
mvn spring-boot:run
```

Once started you can access [swagger](http://localhost:8080/swagger-ui/index.html
) to publish / consume messages.

You can also open the [Confluent Control Center](http://localhost:9021/) to explore the content of topics.

## Stopping the environment
To stop the environment simply run the following command
```bash
docker-compose down -v
```

# Metrics collection
Metrics are collected via micrometer. 
You can choose the backend but this project showcase the prometheus backend.

Metrics are available at http://localhost:8080/actuator/prometheus

We might add a sample Producer/Consumer dashboard in the future.

# Configuration
Most of the configuration is done via traditional [application.yml](src/main/resources/application.yml) file. 
```yaml
kafka:
  properties:
    bootstrap.servers: "localhost:29092"

    schema.registry.url: "http://localhost:8081"
    specific.avro.reader: "true"
  producer:
    key.serializer: "org.apache.kafka.common.serialization.StringSerializer"
    value.serializer: "io.confluent.kafka.serializers.KafkaAvroSerializer"
  consumer:
    key.deserializer: "org.apache.kafka.common.serialization.StringDeserializer"
    value.deserializer: "io.confluent.kafka.serializers.KafkaAvroDeserializer"
    group.id: "kafka-vanilla-spring-boot-demo"
  exceptionHandler: "LogAndFail"
  nbConsumerThreads: 1
```
Basically you have global properties, producer and consumer specific properties.
Every property configured as global (like schema registry here) will be injected in all producers/consumers configuration.

The application accept dynamic property so you can use any of the following properties:
* [Producer Config](https://docs.confluent.io/platform/current/installation/configuration/producer-configs.html)
* [Consumer Config](https://docs.confluent.io/platform/current/installation/configuration/consumer-configs.html)
* Schema Registry Config 
* etc...

By-default the application is configured to use the spring boot application name as an [client.id](https://docs.confluent.io/platform/current/installation/configuration/producer-configs.html#producerconfigs_client.id). 
This will ease monitoring if we have multiple instance of our application.
If needed this can be overridden by specifying the [client.id](https://docs.confluent.io/platform/current/installation/configuration/producer-configs.html#producerconfigs_client.id) property on the producer or consumer config.

## Error handling
The application provides dead letter queue on deserialization / processing error.

The code provide multiple implementation: 
* [LogAndContinue](src/main/java/com/example/demo/kafka/LogAndContinueExceptionHandler.java)
* [LogAndFail](src/main/java/com/example/demo/kafka/LogAndFailExceptionHandler.java)
* [DeadLetterQueue](src/main/java/com/example/demo/kafka/DlqExceptionHandler.java)

By default, the LogAndFail implementation is used.
IT will encourage projects to think about error handling and picking the relevant strategy for their context.

This behavior can be configured via `kafka.exceptionHandler` attribute your application.yml file.
```yaml
kafka:
  exceptionHandler: "LogAndContinue"
```

This implementation will send deserialization and processing errors in the same topic.
Out of the box the topic is called `<spring.application.name>-dlq` but this can be configured in your application.yml file.
```yaml
kafka:
  dlqName: "my-dlq"
```

This implementation will preserve the original :
* headers
* key (as byte[] as we potentially didn't succeed to deserialize it)
* value the (as byte[] as we potentially didn't succeed to deserialize it)
* timestamp 

In addition to this it will add some useful headers :
* `dlq.error.app.name` containing your spring boot application name.
* `dlq.error.timestamp` containing the timestamp of the error
* `dlq.error.topic` containing the source topic
* `dlq.error.partition` containing the source partition
* `dlq.error.offset` containing the source offset
* `dlq.error.exception.class.name` containing the exception class name
* `dlq.error.exception.message` containing the exception message
* `dlq.error.type` containing the error type (either DESERIALIZATION_ERROR or PROCESSING_ERROR)

## Configuring Multiple Consumer Thread
Number of consumer thread is controlled by `kafka.  nbConsumerThreads` attribute your application.yml file.
```yaml
kafka:
  nbConsumerThreads: 1
```
To support multiple thread the class containing your code must :
* extend [AbstractKafkaReader](src/main/java/com/example/demo/services/AbstractKafkaReader.java) class
* be annotated with @Service and @Scope("prototype") (see https://docs.spring.io/spring-framework/docs/current/reference/html/core.html#beans-factory-scopes-prototype to get more details)

Behind the scene [ConsumerAsyncConfiguration](src/main/java/com/example/demo/config/ConsumerAsyncConfiguration.java) will create an executor service with the provided number of threads.
In case of uncaught exception handler, the executor service is configured to stop the application.

# Deep diving in the code
Some important pointers in the code :
* [Properties Loading](src/main/java/com/example/demo/config/KafkaConfig.java)
* [Bean Injection](src/main/java/com/example/demo/config/KafkaLoaderConfiguration.java)
* [Error Handling](src/main/java/com/example/demo/kafka)
* [Producer Example](src/main/java/com/example/demo/services/PaymentPublisher.java)
* [Consumer Example](src/main/java/com/example/demo/services/PaymentReceiver.java)

# How to contribute ?
Have any idea to make showcase better ? Found a bug ? Do not hesitate to report us via github issues and/or create a pull request.

### Reference Documentation
For further reference, please consider the following sections:

* [Official Apache Maven documentation](https://maven.apache.org/guides/index.html)
* [Spring Boot Maven Plugin Reference Guide](https://docs.spring.io/spring-boot/docs/2.7.0/maven-plugin/reference/html/)
* [Create an OCI image](https://docs.spring.io/spring-boot/docs/2.7.0/maven-plugin/reference/html/#build-image)