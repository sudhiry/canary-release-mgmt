package com.canary.payment.handler;

import com.canary.payment.store.ChargeStore;
import com.canary.platform.lib.XCanaryConstants;
import com.canary.platform.lib.XCanaryContext;
import com.canary.platform.lib.XCanaryRestateClientCustomizer;
import com.canary.restate.audit.AuditEvent;
import com.canary.restate.payment.Charge;
import com.canary.restate.payment.ChargeRequest;
import dev.restate.common.Request;
import dev.restate.sdk.CallDurableFuture;
import dev.restate.sdk.ObjectContext;
import dev.restate.sdk.common.StateKey;
import dev.restate.sdk.internal.ContextThreadLocal;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class PaymentVOImplTest {

    ChargeStore store;
    XCanaryRestateClientCustomizer canary;
    PaymentVOImpl handler;
    ObjectContext ctx;

    @BeforeEach
    void setUp() {
        store = new ChargeStore();
        canary = new XCanaryRestateClientCustomizer();
        handler = new PaymentVOImpl(store, canary);
        ctx = mock(ObjectContext.class);
        // Install the mock ObjectContext into the Restate thread-local so Context.current()
        // returns it when handler.charge() calls (ObjectContext) Context.current().
        ContextThreadLocal.setContext(ctx);
    }

    @AfterEach
    void tearDown() {
        ContextThreadLocal.clearContext();
        XCanaryContext.clear();
    }

    @Test
    @SuppressWarnings("unchecked")
    void chargeReturnsExistingWhenStateAlreadySet() {
        Charge existing = new Charge("ch_existing", "ord_1", 999L, "succeeded");
        when(ctx.get(any(StateKey.class))).thenReturn(Optional.of(existing));

        Charge result = handler.charge(new ChargeRequest("ord_1", 999L));

        assertThat(result).isEqualTo(existing);
        // Did not write to store (idempotent shortcut)
        assertThat(store.findById("ch_existing")).isEmpty();
        verify(ctx, never()).set(any(StateKey.class), any());
    }

    @Test
    @SuppressWarnings("unchecked")
    void chargeWritesStateAndStoreAndCallsAuditWhenStateAbsent() {
        when(ctx.get(any(StateKey.class))).thenReturn(Optional.empty());
        when(ctx.call(any(Request.class))).thenReturn(mock(CallDurableFuture.class));

        Charge result = handler.charge(new ChargeRequest("ord_42", 1500L));

        assertThat(result.orderId()).isEqualTo("ord_42");
        assertThat(result.amount()).isEqualTo(1500L);
        assertThat(result.status()).isEqualTo("succeeded");
        assertThat(result.id()).isNotBlank();
        assertThat(store.findById(result.id())).contains(result);

        // Verify state was written
        verify(ctx).set(any(StateKey.class), any(Charge.class));

        // Verify audit Request was sent
        var reqCap = ArgumentCaptor.forClass(Request.class);
        verify(ctx).call(reqCap.capture());
        Request<?, ?> sentReq = reqCap.getValue();
        assertThat(sentReq.getRequest()).isInstanceOf(AuditEvent.class);
        AuditEvent event = (AuditEvent) sentReq.getRequest();
        assertThat(event.aggregate()).isEqualTo("payment");
        assertThat(event.id()).isEqualTo(result.id());
        assertThat(event.action()).isEqualTo("charged");
        assertThat(event.correlationId()).isEqualTo("ord_42");
    }

    @Test
    @SuppressWarnings("unchecked")
    void chargeStampsXCanaryOnAuditCallWhenContextIsCanary() {
        XCanaryContext.set(true);
        when(ctx.get(any(StateKey.class))).thenReturn(Optional.empty());
        when(ctx.call(any(Request.class))).thenReturn(mock(CallDurableFuture.class));

        handler.charge(new ChargeRequest("ord_1", 100L));

        var reqCap = ArgumentCaptor.forClass(Request.class);
        verify(ctx).call(reqCap.capture());
        Request<?, ?> sentReq = reqCap.getValue();
        assertThat(sentReq.getHeaders())
            .containsEntry(XCanaryConstants.HEADER_NAME, XCanaryConstants.TRUE_VALUE);
    }
}
