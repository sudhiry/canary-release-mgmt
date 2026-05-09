package com.canary.audit.store;

import com.canary.restate.audit.AuditEvent;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

@Component
public class AuditEventStore {

    private final List<AuditEvent> events = new CopyOnWriteArrayList<>();

    public void append(AuditEvent event) {
        events.add(event);
    }

    public List<AuditEvent> findByAggregate(String aggregate) {
        return events.stream()
            .filter(e -> aggregate.equals(e.aggregate()))
            .collect(Collectors.toList());
    }

    public List<AuditEvent> all() {
        return new ArrayList<>(events);
    }
}
