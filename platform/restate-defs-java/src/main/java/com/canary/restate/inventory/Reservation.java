package com.canary.restate.inventory;

public record Reservation(
    String id,
    String sku,
    int quantity,
    String orderId,
    String status,
    int bufferUnits   // NEW: 0 stable, 1 canary; derived at response-time, not persisted
) {}
