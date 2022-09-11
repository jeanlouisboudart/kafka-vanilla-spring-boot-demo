package com.example.demo.services;

import com.example.demo.config.KafkaConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.kafka.KafkaStreamsMetrics;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BaseKafkaStreamsApp {

    private static final Logger logger = LoggerFactory.getLogger(BaseKafkaStreamsApp.class);
    private final KafkaConfig kafkaConfig;
    private final MeterRegistry meterRegistry;

    public BaseKafkaStreamsApp(KafkaConfig kafkaConfig, MeterRegistry meterRegistry) {
        this.kafkaConfig = kafkaConfig;
        this.meterRegistry = meterRegistry;
    }

    public <T extends SpecificRecord> Serde<T> specificAvroSerdeKey() {
        final Serde<T> serde = new SpecificAvroSerde<>();
        serde.configure(kafkaConfig.streamsConfig(), true);
        return serde;
    }

    public <T extends SpecificRecord> Serde<T> specificAvroSerdeValue() {
        final Serde<T> serde = new SpecificAvroSerde<>();
        serde.configure(kafkaConfig.streamsConfig(), false);
        return serde;
    }

    public void startTopology() {
        Topology topology = buildTopology();
        logger.info("Starting" + topology.describe());
        KafkaStreams kafkaStreams = new KafkaStreams(topology, new StreamsConfig(kafkaConfig.streamsConfig()));
        new KafkaStreamsMetrics(kafkaStreams).bindTo(meterRegistry);
        kafkaStreams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
    }

    public abstract Topology buildTopology();


}
