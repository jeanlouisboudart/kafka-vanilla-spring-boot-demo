package com.example.demo.services;

import com.example.demo.kafka.DeserializerResult;
import com.example.demo.kafka.KafkaExceptionHandler;
import io.confluent.examples.clients.basicavro.Payment;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.List;

@Component
@Scope("prototype")
public class PaymentReceiver extends AbstractKafkaReader<String, Payment> {
    private final KafkaExceptionHandler<String, Payment> kafkaExceptionHandler;
    private final Logger logger = LoggerFactory.getLogger(PaymentReceiver.class);

    public PaymentReceiver(KafkaConsumer<DeserializerResult<String>, DeserializerResult<Payment>> consumer, KafkaExceptionHandler<String, Payment> kafkaExceptionHandler) {
        super(consumer);
        this.kafkaExceptionHandler = kafkaExceptionHandler;
    }

    public void pollLoop() {
        consumer.subscribe(List.of(PaymentPublisher.TOPIC_NAME));
        while (applicationRunning()) {
            ConsumerRecords<DeserializerResult<String>, DeserializerResult<Payment>> records = consumer.poll(Duration.ofMillis(200));
            for (ConsumerRecord<DeserializerResult<String>, DeserializerResult<Payment>> record : records) {
                kafkaExceptionHandler.handleDeserializationError(record, this::onValidRecord);

            }
        }
    }

    private void onValidRecord(ConsumerRecord<DeserializerResult<String>, DeserializerResult<Payment>> record) {
        //Simulate data manipulation that could result into application errors (NPE)
        try {
            logger.debug("Consumed message key: {}, value: {}", record.key().getDeserializedValue(), record.value().getDeserializedValue().toString());
        } catch (Exception exception) {
            //conversion to DTO expect non-null objects, What will happen if we receive a tombstone ?
            kafkaExceptionHandler.handleProcessingError(record, exception);
        }
    }
}
