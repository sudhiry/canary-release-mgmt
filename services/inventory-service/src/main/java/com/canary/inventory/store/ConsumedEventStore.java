package com.canary.inventory.store;

import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

@Component
public class ConsumedEventStore {

    private final List<ConsumedEvent> events = new CopyOnWriteArrayList<>();

    public void record(ConsumedEvent event) {
        events.add(event);
    }

    public List<ConsumedEvent> all() {
        return new ArrayList<>(events);
    }
}
