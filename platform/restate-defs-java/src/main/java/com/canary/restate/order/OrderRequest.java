package com.canary.restate.order;

public record OrderRequest(String userId, String sku, int quantity, long amount) {}
