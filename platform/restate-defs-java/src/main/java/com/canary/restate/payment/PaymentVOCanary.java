package com.canary.restate.payment;

import dev.restate.sdk.annotation.Handler;
import dev.restate.sdk.annotation.Name;
import dev.restate.sdk.annotation.VirtualObject;

@VirtualObject
@Name("PaymentVOCanary")
public abstract class PaymentVOCanary {
    @Handler
    public abstract Charge charge(ChargeRequest req);

    @Handler
    public abstract Charge refund(ChargeRequest req);
}
