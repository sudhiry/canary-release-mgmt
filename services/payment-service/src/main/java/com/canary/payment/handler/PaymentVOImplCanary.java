package com.canary.payment.handler;

import com.canary.payment.store.ChargeStore;
import com.canary.platform.lib.XCanaryRestateClientCustomizer;
import com.canary.platform.lib.observability.CanaryRestateMeter;
import com.canary.restate.payment.Charge;
import com.canary.restate.payment.ChargeRequest;
import com.canary.restate.payment.PaymentVOCanary;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.kafka.core.KafkaTemplate;

/**
 * Restate-binding subclass for the canary variant. Delegates all handlers to a
 * shared {@link PaymentVOCore} instance; each delegate call is wrapped with
 * {@link CanaryRestateMeter#measure} so per-handler latency + outcome metrics flow.
 */
public class PaymentVOImplCanary extends PaymentVOCanary {
    private final PaymentVOCore core;
    private final CanaryRestateMeter meter;

    public PaymentVOImplCanary(ChargeStore store, XCanaryRestateClientCustomizer canary,
                                KafkaTemplate<String, String> kafkaTemplate,
                                ObjectMapper objectMapper,
                                CanaryRestateMeter meter) {
        this.core = new PaymentVOCore(store, canary, kafkaTemplate, objectMapper, true);
        this.meter = meter;
    }

    @Override
    public Charge charge(ChargeRequest req) {
        try {
            return meter.measure("PaymentVOCanary.charge", () -> core.charge(req));
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Charge refund(ChargeRequest req) {
        try {
            return meter.measure("PaymentVOCanary.refund", () -> core.refund(req));
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
