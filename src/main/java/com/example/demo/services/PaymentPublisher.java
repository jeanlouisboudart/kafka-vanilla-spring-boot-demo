package com.example.demo.services;

import com.example.demo.models.PaymentDTO;
import io.confluent.examples.clients.basicavro.Payment;
import lombok.AllArgsConstructor;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.annotation.PreDestroy;

@Service
@AllArgsConstructor
public class PaymentPublisher {
    private final Logger logger = LoggerFactory.getLogger(PaymentPublisher.class);
    public static final String TOPIC_NAME = "payment";
    private final KafkaProducer<String, Payment> producer;

    public void publish(PaymentDTO paymentDTO) {
        Payment payment = new Payment();
        payment.setId(paymentDTO.getId());
        payment.setAmount(paymentDTO.getAmount());

        ProducerRecord<String, Payment> paymentProducerRecord = new ProducerRecord<>(TOPIC_NAME, paymentDTO.getId(), payment);
        sendMessage(paymentProducerRecord);
    }

    private void sendMessage(ProducerRecord<String, Payment> paymentRecord) {
        producer.send(paymentRecord, this::producerCallBack);
    }

    private void producerCallBack(RecordMetadata recordMetadata, Exception e) {
        if(e != null)
            logger.error("Error publishing payment", e);
        else
            logger.info("Acknowledgement received for {}", recordMetadata);
    }

    @PreDestroy
    public void onShutdown() {
        logger.info("Flushing remaining messages before shutdown");
        producer.flush();
        //producer.close() not required here since Producer interface extends Closeable, Spring will automatically close it.
    }
}
