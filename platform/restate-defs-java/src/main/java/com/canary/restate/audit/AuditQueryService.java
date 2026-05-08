package com.canary.restate.audit;

import dev.restate.sdk.annotation.Handler;
import dev.restate.sdk.annotation.Service;

import java.util.List;

@Service
public abstract class AuditQueryService {
    @Handler
    public abstract void append(AuditEvent event);

    @Handler
    public abstract List<AuditEvent> byAggregate(String aggregateId);
}
