package com.canary.restate.audit;

public record AuditEvent(String aggregate, String id, String action, String correlationId) {}
