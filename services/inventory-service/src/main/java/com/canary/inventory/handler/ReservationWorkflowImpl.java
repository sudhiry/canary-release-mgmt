package com.canary.inventory.handler;

import com.canary.inventory.store.ReservationStore;
import com.canary.platform.lib.XCanaryRestateClientCustomizer;
import com.canary.restate.audit.AuditEvent;
import com.canary.restate.inventory.Reservation;
import com.canary.restate.inventory.ReservationRequest;
import com.canary.restate.inventory.ReservationWorkflow;
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
 * Reservation workflow with awakeable-driven lifecycle.
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
 *
 * <p>Each {@code run()} emits two Kafka events on {@code inventory.events}: the
 * initial {@code reserved} state and the terminal state. Two audit calls fire
 * (once at {@code reserved}, once at the terminal status).
 *
 * <p>The shared handlers {@link #confirm()} / {@link #release()} look up the
 * stored awakeable id and resolve it via
 * {@code ctx.awakeableHandle(id).resolve(TypeTag.of(String.class), value)}. When
 * the state has already been cleared (workflow terminated) they throw
 * {@link TerminalException}.
 *
 * <p>Note on SDK API: Restate Java SDK 2.7.0 does not expose a
 * {@code resolveAwakeable(id, value)} convenience on {@code SharedWorkflowContext};
 * resolution is via {@link dev.restate.sdk.AwakeableHandle#resolve(TypeTag, Object)}.
 * Likewise, the "park on awakeable or timer" idiom uses
 * {@link dev.restate.sdk.DurableFuture#withTimeout(Duration)} rather than the
 * {@code Select.from(...)} helper sketched in early design notes.
 */
public class ReservationWorkflowImpl extends ReservationWorkflow {

    private static final Duration EXPIRY = Duration.ofSeconds(120);
    private static final TypeTag<String> STRING_TAG = TypeTag.of(String.class);
    private static final StateKey<String> AWAKEABLE_ID =
        StateKey.of("awakeableId", String.class);

    private final ReservationStore store;
    private final XCanaryRestateClientCustomizer canary;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public ReservationWorkflowImpl(ReservationStore store, XCanaryRestateClientCustomizer canary,
                                   KafkaTemplate<String, String> kafkaTemplate, ObjectMapper objectMapper) {
        this.store = store;
        this.canary = canary;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    @Override
    public Reservation run(ReservationRequest req) {
        WorkflowContext ctx = (WorkflowContext) Context.current();

        // Initial state: reserved.
        Reservation reserved = new Reservation(
            UUID.randomUUID().toString(), req.sku(), req.quantity(), req.orderId(), "reserved");
        store.put(reserved);
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

        String terminalStatus;
        if ("confirm".equals(outcome)) {
            terminalStatus = "confirmed";
        } else if ("release".equals(outcome)) {
            terminalStatus = "released";
        } else {
            terminalStatus = "expired";
        }

        Reservation terminal = new Reservation(
            reserved.id(), reserved.sku(), reserved.quantity(), reserved.orderId(), terminalStatus);
        store.put(terminal);
        emitInventoryEvent(terminal);
        callAudit(ctx, terminalStatus, terminal.id(), req.orderId());
        return terminal;
    }

    @Override
    public void confirm() {
        SharedWorkflowContext ctx = (SharedWorkflowContext) Context.current();
        Optional<String> id = ctx.get(AWAKEABLE_ID);
        if (id.isEmpty()) {
            throw new TerminalException("reservation not in confirmable state");
        }
        ctx.awakeableHandle(id.get()).resolve(STRING_TAG, "confirm");
    }

    @Override
    public void release() {
        SharedWorkflowContext ctx = (SharedWorkflowContext) Context.current();
        Optional<String> id = ctx.get(AWAKEABLE_ID);
        if (id.isEmpty()) {
            throw new TerminalException("reservation not in releasable state");
        }
        ctx.awakeableHandle(id.get()).resolve(STRING_TAG, "release");
    }

    private void emitInventoryEvent(Reservation reservation) {
        try {
            kafkaTemplate.send("inventory.events", reservation.id(), objectMapper.writeValueAsString(reservation));
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize Reservation", e);
        }
    }

    private void callAudit(WorkflowContext ctx, String action, String reservationId, String orderId) {
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
