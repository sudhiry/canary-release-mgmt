package com.canary.inventory.store;

import com.canary.restate.inventory.Reservation;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;

@Component
public class ReservationStore {
    /** Initial inventory per SKU; thin reference value for 1.3.a. */
    private static final int INITIAL_PER_SKU = 100;

    private final List<Reservation> reservations = new CopyOnWriteArrayList<>();

    public void put(Reservation reservation) { reservations.add(reservation); }

    public Optional<Reservation> findById(String id) {
        return reservations.stream().filter(r -> id.equals(r.id())).findFirst();
    }

    public int availableFor(String sku) {
        int reserved = reservations.stream()
            .filter(r -> sku.equals(r.sku()))
            .mapToInt(Reservation::quantity)
            .sum();
        return Math.max(0, INITIAL_PER_SKU - reserved);
    }

    public List<Reservation> all() { return new ArrayList<>(reservations); }
}
