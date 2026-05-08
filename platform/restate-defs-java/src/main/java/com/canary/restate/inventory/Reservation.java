package com.canary.restate.inventory;

public record Reservation(String id, String sku, int quantity, String orderId, String status) {}
