package com.canary.inventory.handler;

import com.canary.inventory.store.ReservationStore;
import com.canary.platform.lib.XCanaryRestateClientCustomizer;
import com.canary.restate.inventory.Reservation;
import com.canary.restate.inventory.ReservationRequest;
import com.canary.restate.inventory.ReservationWorkflowStable;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.kafka.core.KafkaTemplate;

/**
 * Restate-binding subclass for the stable variant. Delegates all handlers to a
 * shared {@link ReservationWorkflowCore} instance constructed with isCanary=false.
 */
public class ReservationWorkflowImplStable extends ReservationWorkflowStable {
    private final ReservationWorkflowCore core;

    public ReservationWorkflowImplStable(ReservationStore store,
                                          XCanaryRestateClientCustomizer canary,
                                          KafkaTemplate<String, String> kafkaTemplate,
                                          ObjectMapper objectMapper) {
        this.core = new ReservationWorkflowCore(store, canary, kafkaTemplate, objectMapper, false);
    }

    @Override
    public Reservation run(ReservationRequest req) { return core.run(req); }

    @Override
    public Reservation confirm() { return core.confirm(); }

    @Override
    public void release() { core.release(); }
}
