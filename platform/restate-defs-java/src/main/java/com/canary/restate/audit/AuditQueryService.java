package com.canary.restate.audit;

import dev.restate.sdk.annotation.Handler;
import dev.restate.sdk.annotation.Service;

import java.util.List;

/**
 * Restate service contract for audit. Handler methods are POJOs (no Context
 * parameter) per the Restate SDK 2.7 reflection API. Implementations may
 * access the current Context via {@code dev.restate.sdk.Restate.context()}
 * inside the handler body.
 */
@Service
public abstract class AuditQueryService {
    @Handler
    public abstract void append(AuditEvent event);

    @Handler
    public abstract List<AuditEvent> byAggregate(String aggregateId);
}
