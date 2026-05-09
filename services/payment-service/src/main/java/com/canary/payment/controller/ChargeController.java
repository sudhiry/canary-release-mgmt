package com.canary.payment.controller;

import com.canary.payment.store.ChargeStore;
import com.canary.restate.payment.Charge;
import com.canary.restate.payment.ChargeRequest;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.RestClient;

@RestController
public class ChargeController {

    private final RestClient ingressClient;
    private final ChargeStore store;

    public ChargeController(RestClient ingressClient, ChargeStore store) {
        this.ingressClient = ingressClient;
        this.store = store;
    }

    @PostMapping("/charges")
    public ResponseEntity<Charge> create(@RequestBody ChargeRequest req) {
        // VirtualObject is keyed by orderId; Restate Ingress URL: /PaymentVO/{key}/charge
        Charge charge = ingressClient.post()
            .uri("/PaymentVO/{key}/charge", req.orderId())
            .body(req)
            .retrieve()
            .body(Charge.class);
        return ResponseEntity.status(HttpStatus.CREATED).body(charge);
    }

    @GetMapping("/charges/{id}")
    public ResponseEntity<Charge> byId(@PathVariable("id") String id) {
        return store.findById(id)
            .map(ResponseEntity::ok)
            .orElse(ResponseEntity.notFound().build());
    }
}
