package com.canary.restate.inventory;

import dev.restate.sdk.WorkflowContext;
import dev.restate.sdk.annotation.Handler;
import dev.restate.sdk.annotation.Workflow;

@Workflow
public abstract class ReservationWorkflow {
    @Handler
    public abstract Reservation run(WorkflowContext ctx, ReservationRequest req);
}
