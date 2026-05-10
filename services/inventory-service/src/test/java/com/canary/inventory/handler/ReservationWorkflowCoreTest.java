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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
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

class ReservationWorkflowCoreTest {

    ReservationStore store;
    XCanaryRestateClientCustomizer canary;
    @SuppressWarnings("unchecked")
    KafkaTemplate<String, String> kafkaTemplate = mock(KafkaTemplate.class);
    ObjectMapper objectMapper = new ObjectMapper();
    WorkflowContext ctx;
    Awakeable<String> awakeable;
    @SuppressWarnings("unchecked")
    DurableFuture<String> timedAwakeable = mock(DurableFuture.class);

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        store = new ReservationStore();
        canary = new XCanaryRestateClientCustomizer();
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

    private ReservationWorkflowCore core(boolean isCanary) {
        return new ReservationWorkflowCore(store, canary, kafkaTemplate, objectMapper, isCanary);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void runWritesReservedAndAwaitsAwakeable(boolean isCanary) {
        when(timedAwakeable.await()).thenReturn("confirm");
        Reservation result = core(isCanary).run(new ReservationRequest("SKU-A", 5, "ord_1"));
        assertThat(result.sku()).isEqualTo("SKU-A");
        assertThat(result.quantity()).isEqualTo(5);
        assertThat(result.orderId()).isEqualTo("ord_1");
        assertThat(result.status()).isEqualTo("confirmed");
        assertThat(result.bufferUnits()).isEqualTo(isCanary ? 1 : 0);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    @SuppressWarnings("unchecked")
    void confirmSignalTransitionsToConfirmedAndEmitsEvent(boolean isCanary) throws Exception {
        when(timedAwakeable.await()).thenReturn("confirm");
        Reservation result = core(isCanary).run(new ReservationRequest("SKU-A", 5, "ord_1"));
        assertThat(result.status()).isEqualTo("confirmed");
        assertThat(result.bufferUnits()).isEqualTo(isCanary ? 1 : 0);

        var keyCap = ArgumentCaptor.forClass(String.class);
        var valueCap = ArgumentCaptor.forClass(String.class);
        verify(kafkaTemplate, times(2))
            .send(eq("inventory.events"), keyCap.capture(), valueCap.capture());
        Reservation finalEvent = objectMapper.readValue(
            valueCap.getAllValues().get(1), Reservation.class);
        assertThat(finalEvent.status()).isEqualTo("confirmed");
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    @SuppressWarnings("unchecked")
    void releaseSignalTransitionsToReleased(boolean isCanary) throws Exception {
        when(timedAwakeable.await()).thenReturn("release");
        Reservation result = core(isCanary).run(new ReservationRequest("SKU-A", 5, "ord_1"));
        assertThat(result.status()).isEqualTo("released");
        assertThat(result.bufferUnits()).isEqualTo(isCanary ? 1 : 0);

        var valueCap = ArgumentCaptor.forClass(String.class);
        verify(kafkaTemplate, times(2))
            .send(eq("inventory.events"), anyString(), valueCap.capture());
        Reservation finalEvent = objectMapper.readValue(
            valueCap.getAllValues().get(1), Reservation.class);
        assertThat(finalEvent.status()).isEqualTo("released");
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    @SuppressWarnings("unchecked")
    void timerExpiryTransitionsToExpired(boolean isCanary) throws Exception {
        when(timedAwakeable.await()).thenThrow(new TerminalException("timeout"));
        Reservation result = core(isCanary).run(new ReservationRequest("SKU-A", 5, "ord_1"));
        assertThat(result.status()).isEqualTo("expired");
        assertThat(result.bufferUnits()).isEqualTo(isCanary ? 1 : 0);

        var valueCap = ArgumentCaptor.forClass(String.class);
        verify(kafkaTemplate, times(2))
            .send(eq("inventory.events"), anyString(), valueCap.capture());
        Reservation finalEvent = objectMapper.readValue(
            valueCap.getAllValues().get(1), Reservation.class);
        assertThat(finalEvent.status()).isEqualTo("expired");
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    @SuppressWarnings("unchecked")
    void runStampsXCanaryOnAuditCallWhenContextIsCanary(boolean isCanary) {
        XCanaryContext.set(true);
        when(timedAwakeable.await()).thenReturn("confirm");
        core(isCanary).run(new ReservationRequest("SKU-A", 5, "ord_1"));
        var reqCap = ArgumentCaptor.forClass(Request.class);
        verify(ctx, atLeast(1)).call(reqCap.capture());
        Request<?, ?> firstReq = reqCap.getAllValues().get(0);
        assertThat(firstReq.getHeaders())
            .containsEntry(XCanaryConstants.HEADER_NAME, XCanaryConstants.TRUE_VALUE);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    @SuppressWarnings("unchecked")
    void confirmSharedHandlerResolvesAwakeableAndReturnsReservation(boolean isCanary) {
        SharedWorkflowContext sharedCtx = mock(SharedWorkflowContext.class);
        AwakeableHandle handle = mock(AwakeableHandle.class);
        ContextThreadLocal.setContext(sharedCtx);
        when(sharedCtx.get(any(StateKey.class))).thenAnswer(inv -> {
            StateKey<?> key = inv.getArgument(0);
            if ("awakeableId".equals(key.name())) return Optional.of("awk-1");
            if ("currentReservation".equals(key.name())) {
                return Optional.of(new Reservation("res-1", "SKU-A", 5, "ord_1", "reserved", 0));
            }
            return Optional.empty();
        });
        when(sharedCtx.awakeableHandle("awk-1")).thenReturn(handle);

        Reservation result = core(isCanary).confirm();

        verify(handle).resolve(any(TypeTag.class), eq("confirm"));
        assertThat(result.status()).isEqualTo("confirmed");
        assertThat(result.bufferUnits()).isEqualTo(isCanary ? 1 : 0);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    @SuppressWarnings("unchecked")
    void releaseSharedHandlerResolvesAwakeable(boolean isCanary) {
        SharedWorkflowContext sharedCtx = mock(SharedWorkflowContext.class);
        AwakeableHandle handle = mock(AwakeableHandle.class);
        ContextThreadLocal.setContext(sharedCtx);
        when(sharedCtx.get(any(StateKey.class))).thenReturn(Optional.of("awk-1"));
        when(sharedCtx.awakeableHandle("awk-1")).thenReturn(handle);
        core(isCanary).release();
        verify(handle).resolve(any(TypeTag.class), eq("release"));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    @SuppressWarnings("unchecked")
    void confirmAfterTerminationThrowsTerminalException(boolean isCanary) {
        SharedWorkflowContext sharedCtx = mock(SharedWorkflowContext.class);
        ContextThreadLocal.setContext(sharedCtx);
        when(sharedCtx.get(any(StateKey.class))).thenReturn(Optional.empty());
        assertThatThrownBy(() -> core(isCanary).confirm())
            .isInstanceOf(TerminalException.class)
            .hasMessageContaining("not in confirmable state");
    }
}
