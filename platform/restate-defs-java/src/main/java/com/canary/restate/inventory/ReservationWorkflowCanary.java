package com.canary.restate.inventory;

import dev.restate.sdk.annotation.Handler;
import dev.restate.sdk.annotation.Shared;
import dev.restate.sdk.annotation.Workflow;

/**
 * Canary variant. Same handler signatures as the stable variant; differs only
 * in the registered service name. Canary impl returns Reservation with
 * bufferUnits=1 to signal the canary tweak.
 */
@Workflow(name = "ReservationWorkflowCanary")
public abstract class ReservationWorkflowCanary {
    @Handler
    public abstract Reservation run(ReservationRequest req);

    @Shared
    public abstract Reservation confirm();

    @Shared
    public abstract void release();
}
