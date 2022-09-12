package com.example.demo.services;

import com.example.demo.config.ErrorHandler;
import com.example.demo.config.KafkaConfig;
import com.example.demo.kafka.streams.KStreamDeserializationHandler;
import io.confluent.examples.clients.basicavro.Payment;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroDeserializer;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerializer;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.kstream.TimeWindowedDeserializer;
import org.apache.kafka.streams.kstream.Windowed;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Duration;
import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(MockitoExtension.class)
public class PaymentAnalyticTest {

    @Mock
    private MeterRegistry meterRegistry;

    private final KafkaConfig kafkaConfig = new KafkaConfig();

    private TopologyTestDriver topologyTestDriver;


    @BeforeEach
    public void setUp() {
        final Map<String, String> mockProps = new HashMap<>();
        mockProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-vanilla-spring-boot-demo");
        mockProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "DUMMY_KAFKA_BOOTSTRAP_9092");
        mockProps.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://DUMMY_SCHEMA_REGISTRY_8080");
        mockProps.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, KStreamDeserializationHandler.class.getCanonicalName());

        kafkaConfig.setProperties(new HashMap<>());
        kafkaConfig.setStreams(mockProps);

        PaymentAnalytic paymentAnalytic = new PaymentAnalytic(kafkaConfig, meterRegistry);
        Properties streamsProperties = new Properties();
        streamsProperties.putAll(kafkaConfig.streamsConfig());
        topologyTestDriver = new TopologyTestDriver(paymentAnalytic.buildTopology(), streamsProperties);
    }

    @Test
    public void validatePaymentAnalyticCount() {
        TestInputTopic<String, Payment> inputTopic = topologyTestDriver.createInputTopic(PaymentPublisher.TOPIC_NAME,
                new StringSerializer(),
                specificAvroSerializer(false));

        inputTopic.pipeKeyValueList(Arrays.asList(
                new KeyValue<>("1234", new Payment("1234", 10d)),
                new KeyValue<>("1234", new Payment("1234", 1000d))
        ));

        final TestOutputTopic<Windowed<String>, Long> outputTopic = topologyTestDriver.createOutputTopic(PaymentAnalytic.PAYMENT_STATS_TOPIC,
                new TimeWindowedDeserializer<>(new StringDeserializer(), Duration.ofSeconds(30).toMillis()),
                new LongDeserializer());

        final List<KeyValue<Windowed<String>, Long>> results = outputTopic.readKeyValuesToList();
        assertThat(results).hasSize(2);
        assertThat(results.get(0).key.key()).isEqualTo("1234");
        assertThat(results.get(0).value).isEqualTo(1);
        assertThat(results.get(1).key.key()).isEqualTo("1234");
        assertThat(results.get(1).value).isEqualTo(2);
    }


    @Test
    public void shouldLogAndFailWhenDeserializationError() {
        TestInputTopic<String, String> inputTopic = topologyTestDriver.createInputTopic(PaymentPublisher.TOPIC_NAME,
                new StringSerializer(),
                new StringSerializer());

        StreamsException streamsException = Assertions.assertThrows(StreamsException.class, () -> inputTopic.pipeInput("1234", "poison-pill"));
        assertThat(streamsException.getMessage()).isEqualTo("Deserialization exception handler is set to fail upon a deserialization error. If you would rather have the streaming pipeline continue after a deserialization error, please set the default.deserialization.exception.handler appropriately.");

    }

    @Test
    public void shouldLogAndContinueWhenDeserializationError() {
        final Map<String, String> mockProps = new HashMap<>();
        mockProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-vanilla-spring-boot-demo");
        mockProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "DUMMY_KAFKA_BOOTSTRAP_9092");
        mockProps.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://DUMMY_SCHEMA_REGISTRY_8080");
        mockProps.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, KStreamDeserializationHandler.class.getCanonicalName());
        mockProps.put(KStreamDeserializationHandler.DLQ_EXCEPTION_HANDLER_CONFIG, ErrorHandler.LogAndContinue.toString());
        kafkaConfig.setProperties(new HashMap<>());
        kafkaConfig.setStreams(mockProps);

        PaymentAnalytic paymentAnalytic = new PaymentAnalytic(kafkaConfig, meterRegistry);
        Properties streamsProperties = new Properties();
        streamsProperties.putAll(kafkaConfig.streamsConfig());
        topologyTestDriver = new TopologyTestDriver(paymentAnalytic.buildTopology(), streamsProperties);

        TestInputTopic<String, String> inputTopic = topologyTestDriver.createInputTopic(PaymentPublisher.TOPIC_NAME,
                new StringSerializer(),
                new StringSerializer());

        inputTopic.pipeInput("1234", "poison-pill");

        final TestOutputTopic<Windowed<String>, Long> outputTopic = topologyTestDriver.createOutputTopic(PaymentAnalytic.PAYMENT_STATS_TOPIC,
                new TimeWindowedDeserializer<>(new StringDeserializer(), Duration.ofSeconds(30).toMillis()),
                new LongDeserializer());

        final List<KeyValue<Windowed<String>, Long>> results = outputTopic.readKeyValuesToList();
        assertThat(results).isEmpty();
    }

    @Test
    public void shouldFailWhenUncaughtException() {
        TestInputTopic<String, Payment> inputTopic = topologyTestDriver.createInputTopic(PaymentPublisher.TOPIC_NAME,
                new StringSerializer(),
                specificAvroSerializer(false));

        StreamsException streamsException = Assertions.assertThrows(StreamsException.class, () -> inputTopic.pipeInput("BOOM", new Payment("BOOM", 1000d)));
        assertThat(streamsException.getCause().getMessage()).isEqualTo("Should raise uncaught exception");

    }


    private <T extends SpecificRecord> SpecificAvroSerializer<T> specificAvroSerializer(boolean isKey) {
        SpecificAvroSerializer<T> serializer = new SpecificAvroSerializer<>();
        serializer.configure(kafkaConfig.streamsConfig(), isKey);
        return serializer;
    }


    private <T extends SpecificRecord> SpecificAvroDeserializer<T> specificAvroDeserializer(boolean isKey) {
        SpecificAvroDeserializer<T> deserializer = new SpecificAvroDeserializer<>();
        deserializer.configure(kafkaConfig.streamsConfig(), isKey);
        return deserializer;
    }
}