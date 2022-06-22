package com.example.demo.models;

import io.confluent.examples.clients.basicavro.Payment;
import lombok.AllArgsConstructor;
import lombok.Data;

import javax.validation.constraints.NotNull;

@Data
@AllArgsConstructor
public class PaymentDTO {
    private String id;
    private double amount;

    public PaymentDTO(@NotNull Payment payment) {
        this.id = payment.getId().toString();
        this.amount = payment.getAmount();
    }

}
