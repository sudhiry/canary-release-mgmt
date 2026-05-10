package com.canary.restate.inventory;

import dev.restate.sdk.annotation.Handler;
import dev.restate.sdk.annotation.Shared;
import dev.restate.sdk.annotation.Workflow;

/**
 * Restate Workflow contract for inventory reservations. Handler methods are POJOs
 * (no Context parameter) per the Restate SDK 2.7 reflection API. Implementations
 * may access the current WorkflowContext via {@code (WorkflowContext) Context.current()}
 * inside the handler body. Shared handlers access SharedWorkflowContext via
 * {@code (SharedWorkflowContext) Context.current()}.
 *
 * <p>Lifecycle: {@code run()} writes the reservation as {@code reserved} and parks
 * on an awakeable + 120s timer. {@code confirm()} resolves the awakeable with
 * {@code "confirm"}; {@code release()} resolves with {@code "release"}; timer
 * expiry transitions the reservation to {@code expired} as defense in depth.
 * Calling {@code confirm}/{@code release} on a terminated workflow throws
 * {@code TerminalException}.
 */
@Workflow
public abstract class ReservationWorkflow {
    @Handler
    public abstract Reservation run(ReservationRequest req);

    @Shared
    public abstract void confirm();

    @Shared
    public abstract void release();
}
