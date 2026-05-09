package com.canary.audit.controller;

import com.canary.audit.store.AuditEventStore;
import com.canary.restate.audit.AuditEvent;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.RestClient;

import java.util.List;

@RestController
public class AuditController {

    private final RestClient ingressClient;
    private final AuditEventStore store;

    public AuditController(RestClient ingressClient, AuditEventStore store) {
        this.ingressClient = ingressClient;
        this.store = store;
    }

    @PostMapping("/audit/events")
    public ResponseEntity<Void> create(@RequestBody AuditEvent event) {
        ingressClient.post()
            .uri("/AuditQueryService/append")
            .body(event)
            .retrieve()
            .toBodilessEntity();
        return ResponseEntity.status(HttpStatus.CREATED).build();
    }

    @GetMapping("/audit/by-aggregate/{id}")
    public List<AuditEvent> byAggregate(@PathVariable("id") String id) {
        return store.findByAggregate(id);
    }
}
