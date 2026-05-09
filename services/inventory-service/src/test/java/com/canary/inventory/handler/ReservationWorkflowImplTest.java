package com.canary.inventory.handler;

import com.canary.inventory.store.ReservationStore;
import com.canary.platform.lib.XCanaryConstants;
import com.canary.platform.lib.XCanaryContext;
import com.canary.platform.lib.XCanaryRestateClientCustomizer;
import com.canary.restate.audit.AuditEvent;
import com.canary.restate.inventory.Reservation;
import com.canary.restate.inventory.ReservationRequest;
import dev.restate.common.Request;
import dev.restate.sdk.CallDurableFuture;
import dev.restate.sdk.WorkflowContext;
import dev.restate.sdk.internal.ContextThreadLocal;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ReservationWorkflowImplTest {

    ReservationStore store;
    XCanaryRestateClientCustomizer canary;
    ReservationWorkflowImpl handler;
    WorkflowContext ctx;

    @BeforeEach
    void setUp() {
        store = new ReservationStore();
        canary = new XCanaryRestateClientCustomizer();
        handler = new ReservationWorkflowImpl(store, canary);
        ctx = mock(WorkflowContext.class);
        // Install the mock WorkflowContext into the Restate thread-local so Context.current()
        // returns it when handler.run() calls (WorkflowContext) Context.current().
        ContextThreadLocal.setContext(ctx);
    }

    @AfterEach
    void tearDown() {
        ContextThreadLocal.clearContext();
        XCanaryContext.clear();
    }

    @Test
    @SuppressWarnings("unchecked")
    void runRecordsReservationAndCallsAudit() {
        when(ctx.call(any(Request.class))).thenReturn(mock(CallDurableFuture.class));

        Reservation result = handler.run(new ReservationRequest("SKU-A", 5, "ord_42"));

        assertThat(result.sku()).isEqualTo("SKU-A");
        assertThat(result.quantity()).isEqualTo(5);
        assertThat(result.orderId()).isEqualTo("ord_42");
        assertThat(result.status()).isEqualTo("reserved");
        assertThat(result.id()).isNotBlank();
        assertThat(store.findById(result.id())).contains(result);

        // Verify audit Request was sent with correct AuditEvent
        var reqCap = ArgumentCaptor.forClass(Request.class);
        verify(ctx).call(reqCap.capture());
        Request<?, ?> sentReq = reqCap.getValue();
        assertThat(sentReq.getRequest()).isInstanceOf(AuditEvent.class);
        AuditEvent event = (AuditEvent) sentReq.getRequest();
        assertThat(event.aggregate()).isEqualTo("inventory");
        assertThat(event.id()).isEqualTo(result.id());
        assertThat(event.action()).isEqualTo("reserved");
        assertThat(event.correlationId()).isEqualTo("ord_42");
    }

    @Test
    @SuppressWarnings("unchecked")
    void runStampsXCanaryOnAuditCallWhenContextIsCanary() {
        XCanaryContext.set(true);
        when(ctx.call(any(Request.class))).thenReturn(mock(CallDurableFuture.class));

        handler.run(new ReservationRequest("SKU-A", 5, "ord_1"));

        var reqCap = ArgumentCaptor.forClass(Request.class);
        verify(ctx).call(reqCap.capture());
        Request<?, ?> sentReq = reqCap.getValue();
        assertThat(sentReq.getHeaders())
            .containsEntry(XCanaryConstants.HEADER_NAME, XCanaryConstants.TRUE_VALUE);
    }
}
