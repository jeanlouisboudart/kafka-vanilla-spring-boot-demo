package com.example.demo.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.confluent.examples.clients.basicavro.Payment;
import lombok.*;

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
