package com.canary.payment.controller;

import com.canary.payment.store.ChargeStore;
import com.canary.restate.payment.Charge;
import com.canary.restate.payment.ChargeRequest;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.MediaType;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.client.RestClient;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@ExtendWith(MockitoExtension.class)
class ChargeControllerTest {

    @Mock RestClient ingressClient;
    @Mock ChargeStore store;

    MockMvc mockMvc;
    ObjectMapper objectMapper = new ObjectMapper();

    @BeforeEach
    void setUp() {
        var controller = new ChargeController(ingressClient, store);
        mockMvc = MockMvcBuilders.standaloneSetup(controller)
            .setMessageConverters(new MappingJackson2HttpMessageConverter(objectMapper))
            .build();
    }

    @Test
    void postDelegatesToVirtualObjectViaIngress() throws Exception {
        var req = new ChargeRequest("ord_42", 1500L);
        var returned = new Charge("ch_1", "ord_42", 1500L, "succeeded");

        var uriSpec = mock(RestClient.RequestBodyUriSpec.class);
        var bodySpec = mock(RestClient.RequestBodySpec.class);
        var responseSpec = mock(RestClient.ResponseSpec.class);
        when(ingressClient.post()).thenReturn(uriSpec);
        when(uriSpec.uri(eq("/PaymentVO/{key}/charge"), eq("ord_42"))).thenReturn(bodySpec);
        when(bodySpec.body(any(ChargeRequest.class))).thenReturn(bodySpec);
        when(bodySpec.retrieve()).thenReturn(responseSpec);
        when(responseSpec.body(Charge.class)).thenReturn(returned);

        mockMvc.perform(post("/charges")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(req)))
            .andExpect(status().isCreated())
            .andExpect(jsonPath("$.id").value("ch_1"))
            .andExpect(jsonPath("$.status").value("succeeded"));

        var captor = ArgumentCaptor.forClass(ChargeRequest.class);
        verify(bodySpec).body(captor.capture());
        assertThat(captor.getValue()).isEqualTo(req);
    }

    @Test
    void getByIdReturns200WhenFound() throws Exception {
        var charge = new Charge("ch_1", "ord_1", 100L, "succeeded");
        when(store.findById("ch_1")).thenReturn(Optional.of(charge));

        mockMvc.perform(get("/charges/ch_1"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.id").value("ch_1"));
    }

    @Test
    void getByIdReturns404WhenMissing() throws Exception {
        when(store.findById("nope")).thenReturn(Optional.empty());

        mockMvc.perform(get("/charges/nope"))
            .andExpect(status().isNotFound());
    }
}
