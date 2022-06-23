package com.example.demo.services;

import com.example.demo.kafka.KafkaConsumerWithErrorHandling;
import com.example.demo.models.PaymentDTO;
import io.confluent.examples.clients.basicavro.Payment;
import lombok.AllArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

@Service
@AllArgsConstructor
public class PaymentReceiver {
    private final Logger logger = LoggerFactory.getLogger(PaymentReceiver.class);
    private final KafkaConsumerWithErrorHandling<String, Payment> consumer;

    @PostConstruct
    public void init() {
        consumer.subscribe(List.of(PaymentPublisher.TOPIC_NAME));
    }

    public List<PaymentDTO> read() {
        ConsumerRecords<String, Payment> records = consumer.poll(Duration.ofMillis(200));
        List<PaymentDTO> payments = new ArrayList<>();
        for (ConsumerRecord<String, Payment> record : records) {
            payments.add(new PaymentDTO(record.value()));
        }
        return payments;
    }

    @PreDestroy
    public void onShutdown() {
        logger.info("Committing Kafka Messages before shutdown");
        consumer.commitSync();
        //consumer.close() not required here since Consumer interface extends Closeable, Spring will automatically close it.
    }

}
