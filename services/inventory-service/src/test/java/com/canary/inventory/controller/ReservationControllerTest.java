package com.canary.inventory.controller;

import com.canary.inventory.store.ReservationStore;
import com.canary.restate.inventory.Reservation;
import com.canary.restate.inventory.ReservationRequest;
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
class ReservationControllerTest {

    @Mock RestClient ingressClient;
    @Mock ReservationStore store;

    MockMvc mockMvc;
    ObjectMapper objectMapper = new ObjectMapper();

    @BeforeEach
    void setUp() {
        var controller = new ReservationController(ingressClient, store);
        mockMvc = MockMvcBuilders.standaloneSetup(controller)
            .setMessageConverters(new MappingJackson2HttpMessageConverter(objectMapper))
            .build();
    }

    @Test
    void postDelegatesToWorkflowViaIngress() throws Exception {
        var req = new ReservationRequest("SKU-A", 5, "ord_42");
        var returned = new Reservation("res_1", "SKU-A", 5, "ord_42", "reserved");

        var uriSpec = mock(RestClient.RequestBodyUriSpec.class);
        var bodySpec = mock(RestClient.RequestBodySpec.class);
        var responseSpec = mock(RestClient.ResponseSpec.class);
        when(ingressClient.post()).thenReturn(uriSpec);
        when(uriSpec.uri(eq("/ReservationWorkflow/{key}/run"), eq("ord_42"))).thenReturn(bodySpec);
        when(bodySpec.body(any(ReservationRequest.class))).thenReturn(bodySpec);
        when(bodySpec.retrieve()).thenReturn(responseSpec);
        when(responseSpec.body(Reservation.class)).thenReturn(returned);

        mockMvc.perform(post("/reservations")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(req)))
            .andExpect(status().isCreated())
            .andExpect(jsonPath("$.id").value("res_1"))
            .andExpect(jsonPath("$.status").value("reserved"));

        var captor = ArgumentCaptor.forClass(ReservationRequest.class);
        verify(bodySpec).body(captor.capture());
        assertThat(captor.getValue()).isEqualTo(req);
    }

    @Test
    void getAvailabilityReadsFromStore() throws Exception {
        when(store.availableFor("SKU-A")).thenReturn(60);

        mockMvc.perform(get("/products/SKU-A/availability"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.sku").value("SKU-A"))
            .andExpect(jsonPath("$.available").value(60));
    }
}
