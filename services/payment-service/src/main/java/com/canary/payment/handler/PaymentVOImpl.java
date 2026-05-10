package com.canary.payment.handler;

import com.canary.payment.store.ChargeStore;
import com.canary.platform.lib.XCanaryRestateClientCustomizer;
import com.canary.restate.audit.AuditEvent;
import com.canary.restate.payment.Charge;
import com.canary.restate.payment.ChargeRequest;
import com.canary.restate.payment.PaymentVO;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.restate.common.InvocationOptions;
import dev.restate.common.Request;
import dev.restate.common.Target;
import dev.restate.sdk.Context;
import dev.restate.sdk.ObjectContext;
import dev.restate.sdk.common.StateKey;
import dev.restate.sdk.common.TerminalException;
import dev.restate.serde.TypeTag;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.Optional;
import java.util.UUID;

/**
 * Restate VirtualObject handler for payment processing. Keyed by orderId; Restate
 * guarantees per-key serialization so calls with the same orderId run sequentially
 * through the same VO instance.
 *
 * <p>Idempotency: uses a {@link StateKey} named "charge" to persist the first
 * successful Charge. Subsequent calls for the same key return the existing charge
 * without re-processing. {@code refund} flips status to {@code "refunded"} and
 * {@code charge} refuses to recharge a refunded order (cross-handler invariant).
 *
 * <p>Restate-to-Restate audit fan-out: after writing state the handler invokes
 * {@code AuditQueryService.append} via a durable {@link Request}, stamping
 * {@code x-canary} via {@link XCanaryRestateClientCustomizer}.
 */
public class PaymentVOImpl extends PaymentVO {

    private static final StateKey<Charge> CHARGE_STATE =
        StateKey.of("charge", Charge.class);

    private final ChargeStore store;
    private final XCanaryRestateClientCustomizer canary;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public PaymentVOImpl(ChargeStore store, XCanaryRestateClientCustomizer canary,
                         KafkaTemplate<String, String> kafkaTemplate, ObjectMapper objectMapper) {
        this.store = store;
        this.canary = canary;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    @Override
    public Charge charge(ChargeRequest req) {
        ObjectContext ctx = (ObjectContext) Context.current();

        Optional<Charge> existing = ctx.get(CHARGE_STATE);
        if (existing.isPresent()) {
            Charge prior = existing.get();
            if ("refunded".equals(prior.status())) {
                throw new TerminalException("order already refunded; cannot recharge");
            }
            // Idempotent re-entry on a still-succeeded charge.
            return prior;
        }

        Charge charge = new Charge(
            UUID.randomUUID().toString(),
            req.orderId(),
            req.amount(),
            "succeeded"
        );
        ctx.set(CHARGE_STATE, charge);
        store.put(charge);
        emitPaymentsEvent(charge);
        callAudit(ctx, "charged", charge.id(), req.orderId());
        return charge;
    }

    @Override
    public Charge refund(ChargeRequest req) {
        ObjectContext ctx = (ObjectContext) Context.current();

        Optional<Charge> existing = ctx.get(CHARGE_STATE);
        if (existing.isEmpty()) {
            throw new TerminalException("no charge to refund for orderId=" + req.orderId());
        }

        Charge prior = existing.get();
        if ("refunded".equals(prior.status())) {
            // Idempotent re-entry: nothing to do.
            return prior;
        }

        Charge refunded = new Charge(prior.id(), prior.orderId(), prior.amount(), "refunded");
        ctx.set(CHARGE_STATE, refunded);
        store.put(refunded);
        emitPaymentsEvent(refunded);
        callAudit(ctx, "refunded", refunded.id(), refunded.orderId());
        return refunded;
    }

    private void emitPaymentsEvent(Charge charge) {
        try {
            kafkaTemplate.send("payments.events", charge.id(), objectMapper.writeValueAsString(charge));
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize Charge", e);
        }
    }

    private void callAudit(ObjectContext ctx, String action, String chargeId, String orderId) {
        InvocationOptions opts = canary.apply(InvocationOptions.builder());
        var auditReq = Request.of(
                Target.service("AuditQueryService", "append"),
                TypeTag.of(AuditEvent.class),
                TypeTag.of(Void.class),
                new AuditEvent("payment", chargeId, action, orderId)
            ).headers(opts.getHeaders());
        ctx.call(auditReq);
    }
}
