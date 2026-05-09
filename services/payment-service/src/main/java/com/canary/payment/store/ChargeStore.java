package com.canary.payment.store;

import com.canary.restate.payment.Charge;
import org.springframework.stereotype.Component;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Component
public class ChargeStore {

    private final ConcurrentMap<String, Charge> byId = new ConcurrentHashMap<>();

    public void put(Charge charge) {
        byId.put(charge.id(), charge);
    }

    public Optional<Charge> findById(String id) {
        return Optional.ofNullable(byId.get(id));
    }
}
