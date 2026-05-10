package com.canary.payment.handler;

import com.canary.payment.store.ChargeStore;
import com.canary.platform.lib.XCanaryConstants;
import com.canary.platform.lib.XCanaryContext;
import com.canary.platform.lib.XCanaryRestateClientCustomizer;
import com.canary.restate.audit.AuditEvent;
import com.canary.restate.payment.Charge;
import com.canary.restate.payment.ChargeRequest;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.restate.common.Request;
import dev.restate.sdk.CallDurableFuture;
import dev.restate.sdk.ObjectContext;
import dev.restate.sdk.common.StateKey;
import dev.restate.sdk.internal.ContextThreadLocal;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class PaymentVOCoreTest {

    ChargeStore store;
    XCanaryRestateClientCustomizer canary;
    @SuppressWarnings("unchecked")
    KafkaTemplate<String, String> kafkaTemplate = mock(KafkaTemplate.class);
    ObjectMapper objectMapper = new ObjectMapper();
    ObjectContext ctx;

    @BeforeEach
    void setUp() {
        store = new ChargeStore();
        canary = new XCanaryRestateClientCustomizer();
        ctx = mock(ObjectContext.class);
        // Install the mock ObjectContext into the Restate thread-local so Context.current()
        // returns it when core.charge() calls (ObjectContext) Context.current().
        ContextThreadLocal.setContext(ctx);
    }

    @AfterEach
    void tearDown() {
        ContextThreadLocal.clearContext();
        XCanaryContext.clear();
    }

    private PaymentVOCore newCore(boolean isCanary) {
        return new PaymentVOCore(store, canary, kafkaTemplate, objectMapper, isCanary);
    }

    // -------------------------------------------------------------------------
    // Canary-discount math
    // -------------------------------------------------------------------------

    @ParameterizedTest
    @CsvSource({
        "false, 1000, 1000",
        "true,  1000,  990",
        "true,    99,   98",    // integer truncation: (99 * 99) / 100 = 98
        "false,   99,   99",
        "true,   101,   99",    // (101 * 99) / 100 = 99
    })
    @SuppressWarnings("unchecked")
    void chargeAppliesCanaryDiscount(boolean isCanary, long requested, long expectedCharged) {
        when(ctx.get(any(StateKey.class))).thenReturn(Optional.empty());
        when(ctx.call(any(Request.class))).thenReturn(mock(CallDurableFuture.class));

        Charge result = newCore(isCanary).charge(new ChargeRequest("ord-1", requested));

        assertThat(result.amount()).isEqualTo(expectedCharged);
        assertThat(result.status()).isEqualTo("succeeded");
    }

    // -------------------------------------------------------------------------
    // Refund preserves prior.amount (variant-agnostic)
    // -------------------------------------------------------------------------

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    @SuppressWarnings("unchecked")
    void refundFlipsStatusToRefunded(boolean isCanary) throws Exception {
        Charge existing = new Charge("c_1", "ord_1", 100L, "succeeded");
        when(ctx.get(any())).thenReturn(Optional.of(existing));
        when(ctx.call(any(Request.class))).thenReturn(mock(CallDurableFuture.class));

        Charge result = newCore(isCanary).refund(new ChargeRequest("ord_1", 100L));

        assertThat(result.status()).isEqualTo("refunded");
        // Amount preserved from prior (no re-discount)
        assertThat(result.amount()).isEqualTo(100L);
        // State written as refunded
        var stateValueCap = ArgumentCaptor.forClass(Charge.class);
        verify(ctx).set(any(), stateValueCap.capture());
        assertThat(stateValueCap.getValue().status()).isEqualTo("refunded");

        // Kafka refund event emitted
        var keyCap = ArgumentCaptor.forClass(String.class);
        var valueCap = ArgumentCaptor.forClass(String.class);
        verify(kafkaTemplate).send(eq("payments.events"), keyCap.capture(), valueCap.capture());
        assertThat(keyCap.getValue()).isEqualTo("c_1");
        Charge persisted = objectMapper.readValue(valueCap.getValue(), Charge.class);
        assertThat(persisted.status()).isEqualTo("refunded");
    }

    // -------------------------------------------------------------------------
    // Ported tests from PaymentVOImplTest (parameterized where applicable)
    // -------------------------------------------------------------------------

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    @SuppressWarnings("unchecked")
    void chargeReturnsExistingWhenStateAlreadySet(boolean isCanary) {
        Charge existing = new Charge("ch_existing", "ord_1", 999L, "succeeded");
        when(ctx.get(any(StateKey.class))).thenReturn(Optional.of(existing));

        Charge result = newCore(isCanary).charge(new ChargeRequest("ord_1", 999L));

        assertThat(result).isEqualTo(existing);
        // Did not write to store (idempotent shortcut)
        assertThat(store.findById("ch_existing")).isEmpty();
        verify(ctx, never()).set(any(StateKey.class), any());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    @SuppressWarnings("unchecked")
    void chargeWritesStateAndStoreAndCallsAuditWhenStateAbsent(boolean isCanary) {
        when(ctx.get(any(StateKey.class))).thenReturn(Optional.empty());
        when(ctx.call(any(Request.class))).thenReturn(mock(CallDurableFuture.class));

        Charge result = newCore(isCanary).charge(new ChargeRequest("ord_42", 1500L));

        assertThat(result.orderId()).isEqualTo("ord_42");
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

        newCore(false).charge(new ChargeRequest("ord_1", 100L));

        var reqCap = ArgumentCaptor.forClass(Request.class);
        verify(ctx).call(reqCap.capture());
        Request<?, ?> sentReq = reqCap.getValue();
        assertThat(sentReq.getHeaders())
            .containsEntry(XCanaryConstants.HEADER_NAME, XCanaryConstants.TRUE_VALUE);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    @SuppressWarnings("unchecked")
    void chargeEmitsPaymentsEvent(boolean isCanary) throws Exception {
        when(ctx.get(any(StateKey.class))).thenReturn(Optional.empty());
        when(ctx.call(any(Request.class))).thenReturn(mock(CallDurableFuture.class));

        Charge result = newCore(isCanary).charge(new ChargeRequest("ord_42", 1500L));

        var keyCap = ArgumentCaptor.forClass(String.class);
        var valueCap = ArgumentCaptor.forClass(String.class);
        verify(kafkaTemplate).send(eq("payments.events"), keyCap.capture(), valueCap.capture());
        assertThat(keyCap.getValue()).isEqualTo(result.id());
        assertThat(objectMapper.readValue(valueCap.getValue(), Charge.class)).isEqualTo(result);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    @SuppressWarnings("unchecked")
    void refundIsIdempotentWhenAlreadyRefunded(boolean isCanary) {
        Charge alreadyRefunded = new Charge("c_1", "ord_1", 100L, "refunded");
        when(ctx.get(any())).thenReturn(Optional.of(alreadyRefunded));

        Charge result = newCore(isCanary).refund(new ChargeRequest("ord_1", 100L));

        // Same Charge returned, no state write, no Kafka emit, no audit call
        assertThat(result.status()).isEqualTo("refunded");
        assertThat(result.id()).isEqualTo("c_1");
        verify(ctx, org.mockito.Mockito.never()).set(any(), any());
        verify(kafkaTemplate, org.mockito.Mockito.never()).send(any(String.class), any(), any());
        verify(ctx, org.mockito.Mockito.never()).call(any(Request.class));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void refundOnUnchargedOrderThrowsTerminalException(boolean isCanary) {
        when(ctx.get(any())).thenReturn(Optional.empty());

        assertThatThrownBy(() -> newCore(isCanary).refund(new ChargeRequest("ord_1", 100L)))
            .isInstanceOf(dev.restate.sdk.common.TerminalException.class)
            .hasMessageContaining("no charge to refund");
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void chargeAfterRefundThrowsTerminalException(boolean isCanary) {
        Charge alreadyRefunded = new Charge("c_1", "ord_1", 100L, "refunded");
        when(ctx.get(any())).thenReturn(Optional.of(alreadyRefunded));

        assertThatThrownBy(() -> newCore(isCanary).charge(new ChargeRequest("ord_1", 100L)))
            .isInstanceOf(dev.restate.sdk.common.TerminalException.class)
            .hasMessageContaining("already refunded");
    }
}
