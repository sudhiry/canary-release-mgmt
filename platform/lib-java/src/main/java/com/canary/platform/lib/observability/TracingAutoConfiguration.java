package com.canary.platform.lib.observability;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.config.MeterFilter;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.context.annotation.Bean;

/**
 * Tracing-related Micrometer policy. The OTLP exporter and sampler are configured
 * via Spring Boot 4 {@code management.tracing.*} / {@code management.otlp.tracing.*}
 * properties; this class does not duplicate that wiring.
 *
 * <p>The single bean here strips an inherited {@code lane} tag from JVM/process meters.
 * Phase 5 emits {@code lane} only on the four canary-aware meters defined in {@link CanaryMetrics};
 * if a future change accidentally registers a global {@code commonTags("lane", ...)}, this filter
 * removes it from everything except those meters, keeping JVM-meter cardinality bounded.
 */
@AutoConfiguration
public class TracingAutoConfiguration {

    @Bean
    public MeterFilter stripLaneFromNonCanaryMeters() {
        return MeterFilter.deny(id -> {
            String name = id.getName();
            if (name.startsWith("canary_")) return false;
            return id.getTag("lane") != null;
        });
    }
}
