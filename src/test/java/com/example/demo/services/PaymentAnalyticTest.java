package com.example.demo.services;

import com.example.demo.config.KafkaConfig;
import io.confluent.examples.clients.basicavro.Payment;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroDeserializer;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerializer;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.TimeWindowedDeserializer;
import org.apache.kafka.streams.kstream.Windowed;
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
        mockProps.put("application.id", "kafka-vanilla-spring-boot-demo");
        mockProps.put("bootstrap.servers", "DUMMY_KAFKA_BOOTSTRAP_9092");
        mockProps.put("schema.registry.url", "mock://DUMMY_SCHEMA_REGISTRY_8080");
        mockProps.put("default.topic.replication.factor", "1");

        kafkaConfig.setProperties(new HashMap<>());
        kafkaConfig.setStreams(mockProps);

        PaymentAnalytic paymentAnalytic = new PaymentAnalytic(kafkaConfig, meterRegistry);
        Properties streamsProperties = new Properties();
        streamsProperties.putAll(kafkaConfig.streamsConfigAsMap());
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

    private <T extends SpecificRecord> SpecificAvroSerializer<T> specificAvroSerializer(boolean isKey) {
        SpecificAvroSerializer<T> serializer = new SpecificAvroSerializer<>();
        serializer.configure(kafkaConfig.streamsConfigAsMap(), isKey);
        return serializer;
    }


    private <T extends SpecificRecord> SpecificAvroDeserializer<T> specificAvroDeserializer(boolean isKey) {
        SpecificAvroDeserializer<T> deserializer = new SpecificAvroDeserializer<>();
        deserializer.configure(kafkaConfig.streamsConfigAsMap(), isKey);
        return deserializer;
    }
}