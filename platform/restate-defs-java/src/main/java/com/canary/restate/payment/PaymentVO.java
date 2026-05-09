package com.canary.restate.payment;

import dev.restate.sdk.annotation.Handler;
import dev.restate.sdk.annotation.VirtualObject;

/**
 * Restate VirtualObject contract for payment, keyed by orderId. Handler methods
 * are POJOs (no Context parameter) per the Restate SDK 2.7 reflection API.
 * Implementations may access the current ObjectContext via
 * {@code dev.restate.sdk.Context#current()} cast to {@code ObjectContext} inside
 * the handler body.
 */
@VirtualObject
public abstract class PaymentVO {
    @Handler
    public abstract Charge charge(ChargeRequest req);
}
