package com.example.demo.services;

import com.example.demo.config.KafkaConfig;
import com.example.demo.kafka.streams.BaseKafkaStreamsApp;
import io.confluent.examples.clients.basicavro.Payment;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.WindowedSerdes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.time.Duration;

@Service
public class PaymentAnalytic extends BaseKafkaStreamsApp {
    public static final String PAYMENT_STATS_TOPIC = "payment-stats";

    private final Logger logger = LoggerFactory.getLogger(PaymentAnalytic.class);

    public PaymentAnalytic(KafkaConfig kafkaConfig, MeterRegistry meterRegistry, ConfigurableApplicationContext applicationContext) {
        super(kafkaConfig, meterRegistry,applicationContext);
    }

    @PostConstruct
    public void init() {
        startTopology();
    }

    public Topology buildTopology() {
        Serde<String> stringSerde = Serdes.String();
        Serde<Payment> paymentSerde = specificAvroSerdeValue();
        StreamsBuilder builder = new StreamsBuilder();
        Duration windowSize = Duration.ofSeconds(30);
        builder
                .stream(PaymentPublisher.TOPIC_NAME, Consumed.with(stringSerde, paymentSerde))
                .peek((k, v) -> logger.debug("Observed event: key={} value={}", k, v))
                .mapValues(value -> {
                    //simulate a processing error
                    if ("BOOM".equals(value.getId().toString())) {
                        throw new RuntimeException("Should raise uncaught exception");
                    }
                    return value;
                })
                .groupByKey()
                .windowedBy(TimeWindows.ofSizeWithNoGrace(windowSize))
                .count()
                .toStream()
                .peek((k, v) -> logger.debug("Aggregated event: key={} window=[{},{}] value={}", k.key(), k.window().startTime(), k.window().endTime(), v))
                .to(PAYMENT_STATS_TOPIC, Produced.with(WindowedSerdes.timeWindowedSerdeFrom(String.class, windowSize.toMillis()), Serdes.Long()));
        return builder.build();

    }
}
