package com.example.demo.controllers;

import com.example.demo.models.PaymentDTO;
import com.example.demo.services.PaymentPublisher;
import com.example.demo.services.PaymentReceiver;
import lombok.AllArgsConstructor;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@AllArgsConstructor
public class MyEndpointResource {

    private final PaymentPublisher paymentPublisher;
    private final PaymentReceiver paymentReceiver;
    private final KafkaProducer<byte[], byte[]> poisonPillProducer;

    @PostMapping("/payment")
    public ResponseEntity<?> createPayment(@RequestBody PaymentDTO paymentDTO) {
        paymentPublisher.publish(paymentDTO);
        return ResponseEntity.ok().build();
    }

    @GetMapping("/payments")
    public List<PaymentDTO> getPayments() {
        return paymentReceiver.read();
    }

    @GetMapping("/poison-pill")
    public ResponseEntity<?> createPoisonPill(@RequestParam("message") String message) {
        poisonPillProducer.send(new ProducerRecord<>(PaymentPublisher.TOPIC_NAME, message.getBytes()));
        return ResponseEntity.badRequest().build();
    }

    @GetMapping("/tombstone")
    public ResponseEntity<?> createTombstone(@RequestParam("key") String key) {
        poisonPillProducer.send(new ProducerRecord<>(PaymentPublisher.TOPIC_NAME, key.getBytes(), null));
        return ResponseEntity.badRequest().build();
    }
}
