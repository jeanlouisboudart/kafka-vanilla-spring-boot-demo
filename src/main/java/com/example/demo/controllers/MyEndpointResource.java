package com.example.demo.controllers;

import com.example.demo.models.PaymentDTO;
import com.example.demo.services.PaymentPublisher;
import io.swagger.v3.oas.annotations.Operation;
import lombok.AllArgsConstructor;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;


@RestController
@AllArgsConstructor
public class MyEndpointResource {

    private final PaymentPublisher paymentPublisher;
    private final KafkaProducer<byte[], byte[]> poisonPillProducer;

    @PostMapping("/payment")
    @Operation(summary = "Produce a valid payment in Kafka")
    public ResponseEntity<?> createPayment(@RequestBody PaymentDTO paymentDTO) {
        paymentPublisher.publish(paymentDTO);
        return ResponseEntity.ok().build();
    }

    @PostMapping("/poison-pill")
    @Operation(summary = "Produce a poison pill (invalid record) in Kafka, consuming after this might trigger errors")
    public ResponseEntity<?> createPoisonPill(@RequestBody String message) {
        poisonPillProducer.send(new ProducerRecord<>(PaymentPublisher.TOPIC_NAME, message.getBytes()));
        return ResponseEntity.badRequest().build();
    }

    @PostMapping("/tombstone")
    @Operation(summary = "Produce a valid message in Kafka, but this message contains a case (a tombstone) that is not handled by the consumer (processing error)")
    public ResponseEntity<?> createTombstone(@RequestBody String key) {
        poisonPillProducer.send(new ProducerRecord<>(PaymentPublisher.TOPIC_NAME, key.getBytes(), null));
        return ResponseEntity.badRequest().build();
    }
}
