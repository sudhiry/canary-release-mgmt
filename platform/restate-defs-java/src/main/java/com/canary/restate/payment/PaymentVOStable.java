package com.canary.restate.payment;

import dev.restate.sdk.annotation.Handler;
import dev.restate.sdk.annotation.Name;
import dev.restate.sdk.annotation.VirtualObject;

/**
 * Stable variant of payment VO. Registered under "PaymentVOStable" by
 * stable-tagged payment pods. Canary pods register {@link PaymentVOCanary}.
 *
 * <p>Behavior identical to canary except {@code charge.amount} reflects the full
 * requested amount (canary applies 1% discount).
 */
@VirtualObject
@Name("PaymentVOStable")
public abstract class PaymentVOStable {
    @Handler
    public abstract Charge charge(ChargeRequest req);

    @Handler
    public abstract Charge refund(ChargeRequest req);
}
