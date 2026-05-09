package com.canary.payment.controller;

import com.canary.payment.store.ConsumedEvent;
import com.canary.payment.store.ConsumedEventStore;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
public class InternalController {

    private final ConsumedEventStore store;

    public InternalController(ConsumedEventStore store) {
        this.store = store;
    }

    @GetMapping("/internal/consumed-events")
    public List<ConsumedEvent> consumedEvents() {
        return store.all();
    }
}
