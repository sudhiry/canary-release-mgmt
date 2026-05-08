package com.canary.restate.payment;

public record ChargeRequest(String orderId, long amount) {}
