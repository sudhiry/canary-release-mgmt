package com.canary.payment.controller;

import com.canary.payment.store.ConsumedEvent;
import com.canary.payment.store.ConsumedEventStore;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@ExtendWith(MockitoExtension.class)
class InternalControllerTest {

    @Mock ConsumedEventStore store;

    MockMvc mockMvc;

    @BeforeEach
    void setUp() {
        var controller = new InternalController(store);
        mockMvc = MockMvcBuilders.standaloneSetup(controller)
            .setMessageConverters(new MappingJackson2HttpMessageConverter(new ObjectMapper()))
            .build();
    }

    @Test
    void consumedEventsEndpointReturnsRecordedEvents() throws Exception {
        when(store.all()).thenReturn(List.of(
            new ConsumedEvent("orders.events", "ord_1", "{}", Map.of("x-canary", "true"))
        ));

        mockMvc.perform(get("/internal/consumed-events"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$[0].topic").value("orders.events"))
            .andExpect(jsonPath("$[0].key").value("ord_1"))
            .andExpect(jsonPath("$[0].headers['x-canary']").value("true"));
    }
}
