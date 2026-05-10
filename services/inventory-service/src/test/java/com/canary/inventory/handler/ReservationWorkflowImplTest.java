package com.canary.inventory.handler;

import com.canary.inventory.store.ReservationStore;
import com.canary.platform.lib.XCanaryConstants;
import com.canary.platform.lib.XCanaryContext;
import com.canary.platform.lib.XCanaryRestateClientCustomizer;
import com.canary.restate.inventory.Reservation;
import com.canary.restate.inventory.ReservationRequest;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.restate.common.Request;
import dev.restate.sdk.Awakeable;
import dev.restate.sdk.AwakeableHandle;
import dev.restate.sdk.CallDurableFuture;
import dev.restate.sdk.DurableFuture;
import dev.restate.sdk.SharedWorkflowContext;
import dev.restate.sdk.WorkflowContext;
import dev.restate.sdk.common.StateKey;
import dev.restate.sdk.common.TerminalException;
import dev.restate.sdk.internal.ContextThreadLocal;
import dev.restate.serde.TypeTag;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.Duration;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ReservationWorkflowImplTest {

    ReservationStore store;
    XCanaryRestateClientCustomizer canary;
    @SuppressWarnings("unchecked")
    KafkaTemplate<String, String> kafkaTemplate = mock(KafkaTemplate.class);
    ObjectMapper objectMapper = new ObjectMapper();
    ReservationWorkflowImpl handler;
    WorkflowContext ctx;
    Awakeable<String> awakeable;
    @SuppressWarnings("unchecked")
    DurableFuture<String> timedAwakeable = mock(DurableFuture.class);

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        store = new ReservationStore();
        canary = new XCanaryRestateClientCustomizer();
        handler = new ReservationWorkflowImpl(store, canary, kafkaTemplate, objectMapper);
        ctx = mock(WorkflowContext.class);
        awakeable = mock(Awakeable.class);
        when(ctx.awakeable(any(TypeTag.class))).thenReturn(awakeable);
        when(awakeable.id()).thenReturn("awk-1");
        when(awakeable.withTimeout(any(Duration.class))).thenReturn(timedAwakeable);
        when(ctx.call(any(Request.class))).thenReturn(mock(CallDurableFuture.class));
        ContextThreadLocal.setContext(ctx);
    }

    @AfterEach
    void tearDown() {
        ContextThreadLocal.clearContext();
        XCanaryContext.clear();
    }

    @Test
    void runWritesReservedAndAwaitsAwakeable() {
        when(timedAwakeable.await()).thenReturn("confirm");
        Reservation result = handler.run(new ReservationRequest("SKU-A", 5, "ord_1"));
        assertThat(result.sku()).isEqualTo("SKU-A");
        assertThat(result.quantity()).isEqualTo(5);
        assertThat(result.orderId()).isEqualTo("ord_1");
        assertThat(result.status()).isEqualTo("confirmed");
    }

    @Test
    @SuppressWarnings("unchecked")
    void confirmSignalTransitionsToConfirmedAndEmitsEvent() throws Exception {
        when(timedAwakeable.await()).thenReturn("confirm");
        handler.run(new ReservationRequest("SKU-A", 5, "ord_1"));
        var keyCap = ArgumentCaptor.forClass(String.class);
        var valueCap = ArgumentCaptor.forClass(String.class);
        verify(kafkaTemplate, times(2))
            .send(eq("inventory.events"), keyCap.capture(), valueCap.capture());
        Reservation finalEvent = objectMapper.readValue(
            valueCap.getAllValues().get(1), Reservation.class);
        assertThat(finalEvent.status()).isEqualTo("confirmed");
    }

    @Test
    @SuppressWarnings("unchecked")
    void releaseSignalTransitionsToReleased() throws Exception {
        when(timedAwakeable.await()).thenReturn("release");
        Reservation result = handler.run(new ReservationRequest("SKU-A", 5, "ord_1"));
        assertThat(result.status()).isEqualTo("released");
        var valueCap = ArgumentCaptor.forClass(String.class);
        verify(kafkaTemplate, times(2))
            .send(eq("inventory.events"), anyString(), valueCap.capture());
        Reservation finalEvent = objectMapper.readValue(
            valueCap.getAllValues().get(1), Reservation.class);
        assertThat(finalEvent.status()).isEqualTo("released");
    }

    @Test
    @SuppressWarnings("unchecked")
    void timerExpiryTransitionsToExpired() throws Exception {
        // withTimeout(Duration).await() throws TerminalException on timer expiry.
        when(timedAwakeable.await()).thenThrow(new TerminalException("timeout"));
        Reservation result = handler.run(new ReservationRequest("SKU-A", 5, "ord_1"));
        assertThat(result.status()).isEqualTo("expired");
        var valueCap = ArgumentCaptor.forClass(String.class);
        verify(kafkaTemplate, times(2))
            .send(eq("inventory.events"), anyString(), valueCap.capture());
        Reservation finalEvent = objectMapper.readValue(
            valueCap.getAllValues().get(1), Reservation.class);
        assertThat(finalEvent.status()).isEqualTo("expired");
    }

    @Test
    @SuppressWarnings("unchecked")
    void runStampsXCanaryOnAuditCallWhenContextIsCanary() {
        XCanaryContext.set(true);
        when(timedAwakeable.await()).thenReturn("confirm");
        handler.run(new ReservationRequest("SKU-A", 5, "ord_1"));
        var reqCap = ArgumentCaptor.forClass(Request.class);
        verify(ctx, atLeast(1)).call(reqCap.capture());
        Request<?, ?> firstReq = reqCap.getAllValues().get(0);
        assertThat(firstReq.getHeaders())
            .containsEntry(XCanaryConstants.HEADER_NAME, XCanaryConstants.TRUE_VALUE);
    }

    @Test
    @SuppressWarnings("unchecked")
    void confirmSharedHandlerResolvesAwakeable() {
        SharedWorkflowContext sharedCtx = mock(SharedWorkflowContext.class);
        AwakeableHandle handle = mock(AwakeableHandle.class);
        ContextThreadLocal.setContext(sharedCtx);
        when(sharedCtx.get(any(StateKey.class))).thenReturn(Optional.of("awk-1"));
        when(sharedCtx.awakeableHandle("awk-1")).thenReturn(handle);
        handler.confirm();
        verify(handle).resolve(any(TypeTag.class), eq("confirm"));
    }

    @Test
    @SuppressWarnings("unchecked")
    void releaseSharedHandlerResolvesAwakeable() {
        SharedWorkflowContext sharedCtx = mock(SharedWorkflowContext.class);
        AwakeableHandle handle = mock(AwakeableHandle.class);
        ContextThreadLocal.setContext(sharedCtx);
        when(sharedCtx.get(any(StateKey.class))).thenReturn(Optional.of("awk-1"));
        when(sharedCtx.awakeableHandle("awk-1")).thenReturn(handle);
        handler.release();
        verify(handle).resolve(any(TypeTag.class), eq("release"));
    }

    @Test
    @SuppressWarnings("unchecked")
    void confirmAfterTerminationThrowsTerminalException() {
        SharedWorkflowContext sharedCtx = mock(SharedWorkflowContext.class);
        ContextThreadLocal.setContext(sharedCtx);
        when(sharedCtx.get(any(StateKey.class))).thenReturn(Optional.empty());
        assertThatThrownBy(() -> handler.confirm())
            .isInstanceOf(TerminalException.class)
            .hasMessageContaining("not in confirmable state");
    }
}
