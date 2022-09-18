package com.example.demo.kafka.streams;

import com.example.demo.config.KafkaConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.kafka.KafkaStreamsMetrics;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ConfigurableApplicationContext;

public abstract class BaseKafkaStreamsApp {

    private static final Logger logger = LoggerFactory.getLogger(BaseKafkaStreamsApp.class);
    private final KafkaConfig kafkaConfig;
    private final MeterRegistry meterRegistry;
    private final ConfigurableApplicationContext applicationContext;

    public BaseKafkaStreamsApp(KafkaConfig kafkaConfig, MeterRegistry meterRegistry, ConfigurableApplicationContext applicationContext) {
        this.kafkaConfig = kafkaConfig;
        this.meterRegistry = meterRegistry;
        this.applicationContext = applicationContext;
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
        //Shutdown application if there are any error
        kafkaStreams.setUncaughtExceptionHandler(this::uncaughtExceptionHandler);
        kafkaStreams.setStateListener(((newState, oldState) -> {
            if (newState == KafkaStreams.State.PENDING_ERROR) {
                //Stop the app in case of error
                if (applicationContext.isActive()) {
                    SpringApplication.exit(applicationContext, () -> 1);
                }
            }
        }));
        kafkaStreams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
    }

    protected StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse uncaughtExceptionHandler(Throwable exception) {
            logger.error("Uncaught exception occurred in Kafka Streams. Application will shutdown !", exception);
            return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
    }

    public abstract Topology buildTopology();


}
