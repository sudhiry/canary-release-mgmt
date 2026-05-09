package com.canary.audit.controller;

import com.canary.audit.store.AuditEventStore;
import com.canary.restate.audit.AuditEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.client.RestClient;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@ExtendWith(MockitoExtension.class)
class AuditControllerTest {

    @Mock
    RestClient ingressClient;

    @Mock
    AuditEventStore store;

    MockMvc mockMvc;
    ObjectMapper objectMapper = new ObjectMapper();

    @BeforeEach
    void setUp() {
        var controller = new AuditController(ingressClient, store);
        mockMvc = MockMvcBuilders.standaloneSetup(controller)
            .setMessageConverters(new MappingJackson2HttpMessageConverter(objectMapper))
            .build();
    }

    @Test
    void postDelegatesToRestateIngress() throws Exception {
        var event = new AuditEvent("payment", "ch_1", "charged", "ord_1");

        var uriSpec = mock(RestClient.RequestBodyUriSpec.class);
        var bodySpec = mock(RestClient.RequestBodySpec.class);
        var responseSpec = mock(RestClient.ResponseSpec.class);
        when(ingressClient.post()).thenReturn(uriSpec);
        when(uriSpec.uri("/AuditQueryService/append")).thenReturn(bodySpec);
        when(bodySpec.body(any(AuditEvent.class))).thenReturn(bodySpec);
        when(bodySpec.retrieve()).thenReturn(responseSpec);
        when(responseSpec.toBodilessEntity()).thenReturn(ResponseEntity.ok().build());

        mockMvc.perform(post("/audit/events")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(event)))
            .andExpect(status().isCreated());

        var captor = ArgumentCaptor.forClass(AuditEvent.class);
        verify(bodySpec).body(captor.capture());
        assertThat(captor.getValue()).isEqualTo(event);
    }

    @Test
    void getByAggregateReadsStoreDirectly() throws Exception {
        var event = new AuditEvent("ord_1", "evt_1", "created", null);
        when(store.findByAggregate("ord_1")).thenReturn(List.of(event));

        mockMvc.perform(get("/audit/by-aggregate/ord_1"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$[0].id").value("evt_1"))
            .andExpect(jsonPath("$[0].aggregate").value("ord_1"));
    }
}
