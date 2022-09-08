package com.example.demo.services;

import com.example.demo.kafka.DeserializerResult;
import com.example.demo.kafka.KafkaExceptionHandler;
import com.example.demo.models.PaymentDTO;
import io.confluent.examples.clients.basicavro.Payment;
import lombok.AllArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.stereotype.Service;

import java.time.Duration;

@Service
@AllArgsConstructor
@EnableAsync
public class PaymentReceiver {
    private final KafkaConsumer<DeserializerResult<String>, DeserializerResult<Payment>> consumer;
    private final KafkaExceptionHandler<String, Payment> kafkaExceptionHandler;
    private final Logger logger = LoggerFactory.getLogger(PaymentReceiver.class);


    public void readMessages() {
        while (true) {
            ConsumerRecords<DeserializerResult<String>, DeserializerResult<Payment>> records = consumer.poll(Duration.ofMillis(200));
            for (ConsumerRecord<DeserializerResult<String>, DeserializerResult<Payment>> record : records) {
                kafkaExceptionHandler.handleDeserializationError(
                        record,
                        (validRecord) -> onValidRecord(validRecord)
                );

            }
        }
    }

    private void onValidRecord(ConsumerRecord<DeserializerResult<String>, DeserializerResult<Payment>> record) {
        logger.debug("Consumed message key: {}, value: {}", record.key().getDeserializedValue(), record.value().getDeserializedValue().toString());
        //Simulate data manipulation that could result into application errors (NPE)
        try {
            new PaymentDTO(record.value().getDeserializedValue());
        } catch (Exception exception) {
            //conversion to DTO expect non-null objects, What will happen if we receive a tombstone ?
            kafkaExceptionHandler.handleProcessingError(record, exception);
        }

    }

    //Start consumer on startup of the application
    @EventListener(ApplicationReadyEvent.class)
    @Async
    public void onAppStarted() {
        readMessages();
    }
}
