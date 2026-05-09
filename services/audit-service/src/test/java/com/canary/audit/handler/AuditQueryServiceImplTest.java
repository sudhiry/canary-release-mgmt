package com.canary.audit.handler;

import com.canary.audit.store.AuditEventStore;
import com.canary.restate.audit.AuditEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class AuditQueryServiceImplTest {

    AuditEventStore store;
    @SuppressWarnings("unchecked")
    KafkaTemplate<String, String> kafkaTemplate = mock(KafkaTemplate.class);
    ObjectMapper objectMapper = new ObjectMapper();
    AuditQueryServiceImpl handler;

    @BeforeEach
    void setUp() {
        store = new AuditEventStore();
        handler = new AuditQueryServiceImpl(store, kafkaTemplate, objectMapper);
    }

    @Test
    void appendStoresAndEmitsKafka() throws Exception {
        var event = new AuditEvent("ord_1", "evt_1", "created", null);

        handler.append(event);

        assertThat(store.all()).containsExactly(event);

        var keyCap = ArgumentCaptor.forClass(String.class);
        var valueCap = ArgumentCaptor.forClass(String.class);
        verify(kafkaTemplate).send(eq("audit.events"), keyCap.capture(), valueCap.capture());
        assertThat(keyCap.getValue()).isEqualTo("evt_1");
        assertThat(objectMapper.readValue(valueCap.getValue(), AuditEvent.class)).isEqualTo(event);
    }

    @Test
    void byAggregateFiltersByAggregateField() {
        store.append(new AuditEvent("ord_1", "e1", "x", null));
        store.append(new AuditEvent("ord_2", "e2", "x", null));
        store.append(new AuditEvent("ord_1", "e3", "x", null));

        List<AuditEvent> result = handler.byAggregate("ord_1");

        assertThat(result).extracting(AuditEvent::id).containsExactly("e1", "e3");
    }

    @Test
    void byAggregatePreservesInsertionOrder() {
        for (int i = 0; i < 5; i++) {
            store.append(new AuditEvent("a", "e" + i, "x", null));
        }

        List<AuditEvent> result = handler.byAggregate("a");

        assertThat(result).extracting(AuditEvent::id).containsExactly("e0", "e1", "e2", "e3", "e4");
    }
}
