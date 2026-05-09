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
 * without re-processing.
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

        // Idempotency: return existing charge if already processed for this orderId.
        Optional<Charge> existing = ctx.get(CHARGE_STATE);
        if (existing.isPresent()) {
            return existing.get();
        }

        Charge charge = new Charge(
            UUID.randomUUID().toString(),
            req.orderId(),
            req.amount(),
            "succeeded"
        );
        ctx.set(CHARGE_STATE, charge);
        store.put(charge);

        // Emit Kafka event to payments.events
        try {
            kafkaTemplate.send("payments.events", charge.id(), objectMapper.writeValueAsString(charge));
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize Charge", e);
        }

        // Restate-to-Restate: append audit event. Customizer stamps x-canary on headers
        // when the calling thread is in canary context.
        InvocationOptions opts = canary.apply(InvocationOptions.builder());
        var auditReq = Request.of(
                Target.service("AuditQueryService", "append"),
                TypeTag.of(AuditEvent.class),
                TypeTag.of(Void.class),
                new AuditEvent("payment", charge.id(), "charged", req.orderId())
            ).headers(opts.getHeaders());
        ctx.call(auditReq);

        return charge;
    }
}
