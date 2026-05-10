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
 * <p>The single bean here denies registration of any non-canary meter that carries an inherited
 * {@code lane} tag. Phase 5 emits {@code lane} only on the four canary-aware meters defined in
 * {@link CanaryMetrics}; if a future change accidentally registers a global
 * {@code commonTags("lane", ...)}, this filter prevents non-canary meters from polluting the
 * registry. Note: the meter is rejected entirely (not just the tag), so the defensive case
 * results in a missing meter — surface and fix the offending {@code commonTags} call rather
 * than relying on this filter as a long-term safety net.
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
