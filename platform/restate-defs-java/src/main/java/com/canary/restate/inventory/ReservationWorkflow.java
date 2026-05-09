package com.canary.restate.inventory;

import dev.restate.sdk.annotation.Handler;
import dev.restate.sdk.annotation.Workflow;

/**
 * Restate Workflow contract for inventory reservations. Handler methods are POJOs
 * (no Context parameter) per the Restate SDK 2.7 reflection API. Implementations
 * may access the current WorkflowContext via {@code (WorkflowContext) Context.current()}
 * inside the handler body.
 */
@Workflow
public abstract class ReservationWorkflow {
    @Handler
    public abstract Reservation run(ReservationRequest req);
}
