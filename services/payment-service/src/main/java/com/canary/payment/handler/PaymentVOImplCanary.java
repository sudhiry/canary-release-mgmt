package com.canary.payment.handler;

import com.canary.payment.store.ChargeStore;
import com.canary.platform.lib.XCanaryRestateClientCustomizer;
import com.canary.restate.payment.Charge;
import com.canary.restate.payment.ChargeRequest;
import com.canary.restate.payment.PaymentVOCanary;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.kafka.core.KafkaTemplate;

/**
 * Restate-binding subclass for the canary variant. Delegates all handlers to a
 * shared {@link PaymentVOCore} instance constructed with isCanary=true.
 */
public class PaymentVOImplCanary extends PaymentVOCanary {
    private final PaymentVOCore core;

    public PaymentVOImplCanary(ChargeStore store, XCanaryRestateClientCustomizer canary,
                                KafkaTemplate<String, String> kafkaTemplate,
                                ObjectMapper objectMapper) {
        this.core = new PaymentVOCore(store, canary, kafkaTemplate, objectMapper, true);
    }

    @Override
    public Charge charge(ChargeRequest req) { return core.charge(req); }

    @Override
    public Charge refund(ChargeRequest req) { return core.refund(req); }
}
