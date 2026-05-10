package com.canary.restate.inventory;

import dev.restate.sdk.annotation.Handler;
import dev.restate.sdk.annotation.Shared;
import dev.restate.sdk.annotation.Workflow;

/**
 * Stable variant of the inventory reservation workflow. Registered under the
 * Restate service name "ReservationWorkflowStable" by stable-tagged inventory
 * pods. Canary pods register {@link ReservationWorkflowCanary}.
 *
 * <p>Lifecycle is identical to the canary variant; only the registered service
 * name differs. Implementations delegate to {@code ReservationWorkflowCore}.
 */
@Workflow(name = "ReservationWorkflowStable")
public abstract class ReservationWorkflowStable {
    @Handler
    public abstract Reservation run(ReservationRequest req);

    @Shared
    public abstract Reservation confirm();   // returns confirmed Reservation; was void in Phase 3.a

    @Shared
    public abstract void release();
}
