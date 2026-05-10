package com.canary.inventory.handler;

import com.canary.inventory.store.ReservationStore;
import com.canary.platform.lib.XCanaryRestateClientCustomizer;
import com.canary.restate.audit.AuditEvent;
import com.canary.restate.inventory.Reservation;
import com.canary.restate.inventory.ReservationRequest;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.restate.common.InvocationOptions;
import dev.restate.common.Request;
import dev.restate.common.Target;
import dev.restate.sdk.Awakeable;
import dev.restate.sdk.Context;
import dev.restate.sdk.SharedWorkflowContext;
import dev.restate.sdk.WorkflowContext;
import dev.restate.sdk.common.StateKey;
import dev.restate.sdk.common.TerminalException;
import dev.restate.serde.TypeTag;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.Duration;
import java.util.Optional;
import java.util.UUID;

/**
 * Shared logic for the reservation workflow. Not a Restate handler binding —
 * thin delegate subclasses ({@link ReservationWorkflowImplStable},
 * {@link ReservationWorkflowImplCanary}) extend the appropriate abstract class
 * and forward every call here.
 *
 * <p>The {@code isCanary} flag controls {@link #withVariant(Reservation)}: stable
 * returns {@code bufferUnits=0}; canary returns {@code bufferUnits=1}. State
 * persisted to Restate always uses {@code bufferUnits=0}; the variant stamp is
 * applied only on return paths.
 *
 * <p>Lifecycle:
 * <ul>
 *   <li>{@code run()} writes the reservation as {@code reserved}, registers a
 *       {@link Awakeable Awakeable&lt;String&gt;}, persists its id under
 *       {@link #AWAKEABLE_ID}, and parks on {@code awakeable.withTimeout(120s).await()}.
 *   <li>If a sibling shared handler resolves the awakeable with {@code "confirm"}
 *       or {@code "release"}, the workflow transitions to {@code confirmed} or
 *       {@code released} respectively.
 *   <li>If the 120s timeout elapses first the underlying SDK throws
 *       {@link TerminalException} from {@code await()}; we treat that as
 *       {@code expired}.
 * </ul>
 */
public class ReservationWorkflowCore {

    private static final Duration EXPIRY = Duration.ofSeconds(120);
    private static final TypeTag<String> STRING_TAG = TypeTag.of(String.class);
    private static final StateKey<String> AWAKEABLE_ID =
        StateKey.of("awakeableId", String.class);
    private static final StateKey<Reservation> CURRENT_RESERVATION =
        StateKey.of("currentReservation", Reservation.class);

    private final ReservationStore store;
    private final XCanaryRestateClientCustomizer canary;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final boolean isCanary;

    public ReservationWorkflowCore(ReservationStore store,
                                   XCanaryRestateClientCustomizer canary,
                                   KafkaTemplate<String, String> kafkaTemplate,
                                   ObjectMapper objectMapper,
                                   boolean isCanary) {
        this.store = store;
        this.canary = canary;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.isCanary = isCanary;
    }

    public Reservation run(ReservationRequest req) {
        WorkflowContext ctx = (WorkflowContext) Context.current();

        // Initial state: reserved. Store with bufferUnits=0 (variant-agnostic in state).
        Reservation reserved = new Reservation(
            UUID.randomUUID().toString(), req.sku(), req.quantity(), req.orderId(), "reserved", 0);
        store.put(reserved);
        ctx.set(CURRENT_RESERVATION, reserved);
        emitInventoryEvent(reserved);
        callAudit(ctx, "reserved", reserved.id(), req.orderId());

        // Park on awakeable with a 120s deadline. The deadline yields a TerminalException
        // from await(); a sibling confirm/release handler resolves with "confirm"/"release".
        Awakeable<String> signal = ctx.awakeable(STRING_TAG);
        ctx.set(AWAKEABLE_ID, signal.id());

        String outcome;
        try {
            outcome = signal.withTimeout(EXPIRY).await();
        } catch (TerminalException e) {
            outcome = null;
        }

        // Workflow terminated: clear the awakeable id so late confirm/release calls
        // see the empty state and reject with TerminalException.
        ctx.clear(AWAKEABLE_ID);
        ctx.clear(CURRENT_RESERVATION);

        String terminalStatus;
        if ("confirm".equals(outcome)) {
            terminalStatus = "confirmed";
        } else if ("release".equals(outcome)) {
            terminalStatus = "released";
        } else {
            terminalStatus = "expired";
        }

        // Store with bufferUnits=0 (variant-agnostic in state).
        Reservation terminal = new Reservation(
            reserved.id(), reserved.sku(), reserved.quantity(), reserved.orderId(), terminalStatus, 0);
        store.put(terminal);
        emitInventoryEvent(terminal);
        callAudit(ctx, terminalStatus, terminal.id(), req.orderId());
        return withVariant(terminal);
    }

    public Reservation confirm() {
        SharedWorkflowContext ctx = (SharedWorkflowContext) Context.current();
        Optional<String> id = ctx.get(AWAKEABLE_ID);
        if (id.isEmpty()) {
            throw new TerminalException("reservation not in confirmable state");
        }
        ctx.awakeableHandle(id.get()).resolve(STRING_TAG, "confirm");
        // Read current reservation state and return with variant stamp.
        Optional<Reservation> current = ctx.get(CURRENT_RESERVATION);
        Reservation base = current.orElseThrow(() ->
            new TerminalException("reservation state not found after confirm signal"));
        return withVariant(new Reservation(
            base.id(), base.sku(), base.quantity(), base.orderId(), "confirmed", 0));
    }

    public void release() {
        SharedWorkflowContext ctx = (SharedWorkflowContext) Context.current();
        Optional<String> id = ctx.get(AWAKEABLE_ID);
        if (id.isEmpty()) {
            throw new TerminalException("reservation not in releasable state");
        }
        ctx.awakeableHandle(id.get()).resolve(STRING_TAG, "release");
    }

    /**
     * Stamps the variant-specific bufferUnits onto a Reservation before returning
     * to the caller. State is always stored with bufferUnits=0; this is applied
     * only on return paths.
     */
    private Reservation withVariant(Reservation r) {
        return new Reservation(r.id(), r.sku(), r.quantity(), r.orderId(), r.status(),
                               isCanary ? 1 : 0);
    }

    private void emitInventoryEvent(Reservation reservation) {
        try {
            kafkaTemplate.send("inventory.events", reservation.id(),
                objectMapper.writeValueAsString(reservation));
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize Reservation", e);
        }
    }

    private void callAudit(WorkflowContext ctx, String action, String reservationId,
                           String orderId) {
        InvocationOptions opts = canary.apply(InvocationOptions.builder());
        var auditReq = Request.of(
                Target.service("AuditQueryService", "append"),
                TypeTag.of(AuditEvent.class),
                TypeTag.of(Void.class),
                new AuditEvent("inventory", reservationId, action, orderId)
            ).headers(opts.getHeaders());
        ctx.call(auditReq);
    }
}
