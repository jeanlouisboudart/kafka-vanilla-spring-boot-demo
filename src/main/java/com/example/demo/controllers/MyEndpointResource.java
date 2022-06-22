package com.example.demo.controllers;

import com.example.demo.models.PaymentDTO;
import com.example.demo.services.PaymentPublisher;
import com.example.demo.services.PaymentReceiver;
import io.confluent.examples.clients.basicavro.Payment;
import lombok.AllArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@AllArgsConstructor
public class MyEndpointResource {

    private final PaymentPublisher paymentPublisher;
    private final PaymentReceiver paymentReceiver;

    @PostMapping("/payment")
    public ResponseEntity createPayment(@RequestBody PaymentDTO paymentDTO) {
        paymentPublisher.publish(paymentDTO);
        return ResponseEntity.ok().build();
    }

    @GetMapping("/payments")
    public List<PaymentDTO> getPayments() {
        return paymentReceiver.read();
    }
}
