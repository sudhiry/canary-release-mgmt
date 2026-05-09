package com.canary.inventory.handler;

import com.canary.inventory.store.ReservationStore;
import com.canary.platform.lib.XCanaryRestateClientCustomizer;
import com.canary.restate.audit.AuditEvent;
import com.canary.restate.inventory.Reservation;
import com.canary.restate.inventory.ReservationRequest;
import com.canary.restate.inventory.ReservationWorkflow;
import dev.restate.common.InvocationOptions;
import dev.restate.common.Request;
import dev.restate.common.Target;
import dev.restate.sdk.Context;
import dev.restate.sdk.WorkflowContext;
import dev.restate.serde.TypeTag;

import java.util.UUID;

public class ReservationWorkflowImpl extends ReservationWorkflow {

    private final ReservationStore store;
    private final XCanaryRestateClientCustomizer canary;

    public ReservationWorkflowImpl(ReservationStore store, XCanaryRestateClientCustomizer canary) {
        this.store = store;
        this.canary = canary;
    }

    @Override
    public Reservation run(ReservationRequest req) {
        WorkflowContext ctx = (WorkflowContext) Context.current();

        // Trimmed-C: just record the reservation. No timer / release-on-expiry — Phase 3.
        Reservation reservation = new Reservation(
            UUID.randomUUID().toString(),
            req.sku(),
            req.quantity(),
            req.orderId(),
            "reserved"
        );
        store.put(reservation);

        // Restate-to-Restate audit fan-out. Customizer stamps x-canary on headers.
        InvocationOptions opts = canary.apply(InvocationOptions.builder());
        var auditReq = Request.of(
                Target.service("AuditQueryService", "append"),
                TypeTag.of(AuditEvent.class),
                TypeTag.of(Void.class),
                new AuditEvent("inventory", reservation.id(), "reserved", req.orderId())
            ).headers(opts.getHeaders());
        ctx.call(auditReq);

        return reservation;
    }
}
