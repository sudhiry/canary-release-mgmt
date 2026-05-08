package com.canary.restate.order;

public record Order(String id, String userId, String sku, int quantity, long amount, String status) {}
