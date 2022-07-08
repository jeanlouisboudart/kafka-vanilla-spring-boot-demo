package com.example.demo.controllers;

import com.example.demo.models.PaymentDTO;
import com.example.demo.services.PaymentPublisher;
import com.example.demo.services.PaymentReceiver;
import io.swagger.v3.oas.annotations.Operation;
import lombok.AllArgsConstructor;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@AllArgsConstructor
public class MyEndpointResource {

    private final PaymentPublisher paymentPublisher;
    private final PaymentReceiver paymentReceiver;
    private final KafkaProducer<byte[], byte[]> poisonPillProducer;

    @GetMapping("/payments")
    @Operation(summary = "Consume payments")
    public List<PaymentDTO> getPayments() {
        return paymentReceiver.read();
    }

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
