package com.canary.inventory.controller;

import com.canary.inventory.store.ReservationStore;
import com.canary.restate.inventory.AvailabilityResponse;
import com.canary.restate.inventory.Reservation;
import com.canary.restate.inventory.ReservationRequest;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.RestClient;

@RestController
public class ReservationController {

    private final RestClient ingressClient;
    private final ReservationStore store;

    public ReservationController(RestClient ingressClient, ReservationStore store) {
        this.ingressClient = ingressClient;
        this.store = store;
    }

    @PostMapping("/reservations")
    public ResponseEntity<Reservation> create(@RequestBody ReservationRequest req) {
        Reservation reservation = ingressClient.post()
            .uri("/ReservationWorkflow/{key}/run", req.orderId())
            .body(req)
            .retrieve()
            .body(Reservation.class);
        return ResponseEntity.status(HttpStatus.CREATED).body(reservation);
    }

    @GetMapping("/products/{sku}/availability")
    public AvailabilityResponse availability(@PathVariable("sku") String sku) {
        return new AvailabilityResponse(sku, store.availableFor(sku));
    }
}
