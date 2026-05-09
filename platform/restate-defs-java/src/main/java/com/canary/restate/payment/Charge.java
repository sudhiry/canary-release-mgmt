package com.canary.restate.payment;

public record Charge(String id, String orderId, long amount, String status) {}
