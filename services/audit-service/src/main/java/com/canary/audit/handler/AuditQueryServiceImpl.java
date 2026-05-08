package com.canary.audit.handler;

import com.canary.audit.store.AuditEventStore;
import com.canary.restate.audit.AuditEvent;
import com.canary.restate.audit.AuditQueryService;

import java.util.List;

public class AuditQueryServiceImpl extends AuditQueryService {

    private final AuditEventStore store;

    public AuditQueryServiceImpl(AuditEventStore store) {
        this.store = store;
    }

    @Override
    public void append(AuditEvent event) {
        store.append(event);
        // Kafka emission added in Task 7.
    }

    @Override
    public List<AuditEvent> byAggregate(String aggregateId) {
        return store.findByAggregate(aggregateId);
    }
}
