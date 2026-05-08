package com.canary.restate.payment;

import dev.restate.sdk.ObjectContext;
import dev.restate.sdk.annotation.Handler;
import dev.restate.sdk.annotation.VirtualObject;

@VirtualObject
public abstract class PaymentVO {
    @Handler
    public abstract Charge charge(ObjectContext ctx, ChargeRequest req);
}
