package com.canary.audit.handler;

import com.canary.audit.store.AuditEventStore;
import com.canary.restate.audit.AuditEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class AuditQueryServiceImplTest {

    AuditEventStore store;
    AuditQueryServiceImpl handler;

    @BeforeEach
    void setUp() {
        store = new AuditEventStore();
        handler = new AuditQueryServiceImpl(store);
    }

    @Test
    void appendStoresTheEvent() {
        var event = new AuditEvent("ord_1", "evt_1", "created", null);

        handler.append(event);

        assertThat(store.all()).containsExactly(event);
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
