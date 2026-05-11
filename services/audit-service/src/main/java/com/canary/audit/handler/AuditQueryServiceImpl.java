package com.canary.audit.handler;

import com.canary.audit.store.AuditEventStore;
import com.canary.platform.lib.observability.CanaryRestateMeter;
import com.canary.restate.audit.AuditEvent;
import com.canary.restate.audit.AuditQueryService;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.List;

public class AuditQueryServiceImpl extends AuditQueryService {

    private final AuditEventStore store;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final CanaryRestateMeter meter;

    public AuditQueryServiceImpl(AuditEventStore store,
                                 KafkaTemplate<String, String> kafkaTemplate,
                                 ObjectMapper objectMapper,
                                 CanaryRestateMeter meter) {
        this.store = store;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.meter = meter;
    }

    @Override
    public void append(AuditEvent event) {
        try {
            meter.measure("AuditQueryService.append", () -> {
                store.append(event);
                try {
                    String json = objectMapper.writeValueAsString(event);
                    kafkaTemplate.send("audit.events", event.id(), json);
                } catch (JsonProcessingException e) {
                    throw new RuntimeException("Failed to serialize AuditEvent", e);
                }
                return null;
            });
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<AuditEvent> byAggregate(String aggregateId) {
        try {
            return meter.measure("AuditQueryService.byAggregate", () -> store.findByAggregate(aggregateId));
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
