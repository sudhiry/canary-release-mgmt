package com.canary.restate.inventory;

public record ReservationRequest(String sku, int quantity, String orderId) {}
