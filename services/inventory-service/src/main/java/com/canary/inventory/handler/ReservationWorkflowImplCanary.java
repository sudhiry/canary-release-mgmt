package com.canary.inventory.handler;

import com.canary.inventory.store.ReservationStore;
import com.canary.platform.lib.XCanaryRestateClientCustomizer;
import com.canary.platform.lib.observability.CanaryRestateMeter;
import com.canary.restate.inventory.Reservation;
import com.canary.restate.inventory.ReservationRequest;
import com.canary.restate.inventory.ReservationWorkflowCanary;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.kafka.core.KafkaTemplate;

/**
 * Restate-binding subclass for the canary variant. Delegates all handlers to a
 * shared {@link ReservationWorkflowCore} instance; each delegate call is wrapped with
 * {@link CanaryRestateMeter#measure} so per-handler latency + outcome metrics flow.
 */
public class ReservationWorkflowImplCanary extends ReservationWorkflowCanary {
    private final ReservationWorkflowCore core;
    private final CanaryRestateMeter meter;

    public ReservationWorkflowImplCanary(ReservationStore store,
                                          XCanaryRestateClientCustomizer canary,
                                          KafkaTemplate<String, String> kafkaTemplate,
                                          ObjectMapper objectMapper,
                                          CanaryRestateMeter meter) {
        this.core = new ReservationWorkflowCore(store, canary, kafkaTemplate, objectMapper, true);
        this.meter = meter;
    }

    @Override
    public Reservation run(ReservationRequest req) {
        try {
            return meter.measure("ReservationWorkflowCanary.run", () -> core.run(req));
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Reservation confirm() {
        try {
            return meter.measure("ReservationWorkflowCanary.confirm", () -> core.confirm());
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void release() {
        try {
            meter.measure("ReservationWorkflowCanary.release", () -> { core.release(); return null; });
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
