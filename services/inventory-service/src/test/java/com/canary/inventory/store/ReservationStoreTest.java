package com.canary.inventory.store;

import com.canary.restate.inventory.Reservation;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ReservationStoreTest {

    ReservationStore store;

    @BeforeEach
    void setUp() {
        store = new ReservationStore();
    }

    @Test
    void availableForUnreservedSkuReturns100() {
        assertThat(store.availableFor("SKU-A")).isEqualTo(100);
    }

    @Test
    void availableForSubtractsReservations() {
        store.put(new Reservation("r1", "SKU-A", 30, "ord_1", "reserved", 0));
        store.put(new Reservation("r2", "SKU-A", 20, "ord_2", "reserved", 0));
        assertThat(store.availableFor("SKU-A")).isEqualTo(50);
    }

    @Test
    void availableForReturnsZeroWhenOverdrawn() {
        store.put(new Reservation("r1", "SKU-A", 110, "ord_1", "reserved", 0));
        assertThat(store.availableFor("SKU-A")).isEqualTo(0);
    }

    @Test
    void availableForIsolatesSkusFromEachOther() {
        store.put(new Reservation("r1", "SKU-A", 40, "ord_1", "reserved", 0));
        // SKU-B untouched; SKU-A reserved 40
        assertThat(store.availableFor("SKU-B")).isEqualTo(100);
        assertThat(store.availableFor("SKU-A")).isEqualTo(60);
    }
}
