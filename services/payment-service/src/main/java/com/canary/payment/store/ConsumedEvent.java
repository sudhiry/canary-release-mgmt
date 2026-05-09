package com.canary.payment.store;

import java.util.Map;

public record ConsumedEvent(String topic, String key, String value, Map<String, String> headers) {}
