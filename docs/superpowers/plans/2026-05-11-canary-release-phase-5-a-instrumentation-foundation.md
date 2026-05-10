# Canary Release Phase 5.a — Instrumentation Foundation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Lay the observability foundation in `platform/lib-java`: define the four canary-aware Micrometer metrics from §1 of the spec, auto-emit HTTP-substrate metrics, register a Kafka `RecordInterceptor` that emits Kafka-substrate metrics, expose an interceptor helper for Restate handler wiring (used in 5.b), watch the k8s Service selector to emit the `canary_lane_active` gauge, and wire the OpenTelemetry tracer bridge so app spans land in Jaeger with the `canary.lane` attribute on HTTP entry. Update each of the five services' `application.yml` to expose Prometheus and configure tracing. Add scrape annotations to the shared Helm deployment template.

**Architecture:** A new package `com.canary.platform.lib.observability` in `platform/lib-java`. One `CanaryMetrics` helper class centralises meter creation; one `CanaryLaneTag` enum + helper resolves the `lane` tag value from `XCanaryContext`. HTTP metrics are emitted by augmenting the existing `XCanaryRequestFilter`. Kafka metrics are emitted by a new `CanaryKafkaRecordInterceptor` registered on the `kafkaListenerContainerFactory` bean. Restate metrics are emitted by a new `CanaryRestateMeter` helper that 5.b will wire into each handler. `LaneStateProbe` is a Fabric8 k8s-client watcher modelled on the existing `XCanaryPresenceWatcher`; it emits `canary_lane_active` as a `MultiGauge`. `TracingAutoConfiguration` registers the OTel tracer bridge and an HTTP server `ObservationFilter` that adds the `canary.lane` attribute to the active span. No infra changes; service code changes are limited to `application.yml`.

**Tech Stack:** Java 25, Spring Boot 4.0.4, Micrometer (bundled with `spring-boot-starter-actuator`), `micrometer-tracing-bridge-otel`, `opentelemetry-exporter-otlp` (gRPC), `io.opentelemetry.instrumentation:opentelemetry-spring-boot-starter` for HTTP server/client auto-instrumentation, `io.fabric8:kubernetes-client:7.4.0` (already present), JUnit 5 + Mockito + AssertJ for tests.

---

## File Structure

### Created

| Path | Responsibility |
|---|---|
| `platform/lib-java/src/main/java/com/canary/platform/lib/observability/CanaryLaneTag.java` | Enum (`STABLE`, `CANARY`) + helper `current()` that reads `XCanaryContext` and returns the tag value string. |
| `platform/lib-java/src/main/java/com/canary/platform/lib/observability/CanaryMetrics.java` | Holds the `MeterRegistry`. Public API: `recordHttp(...)`, `recordKafka(...)`, `recordRestate(...)`, `recordShadowMismatch(service, field)`. Internally builds the four metric names with the standard tag set. |
| `platform/lib-java/src/main/java/com/canary/platform/lib/observability/CanaryKafkaRecordInterceptor.java` | Implements `org.springframework.kafka.listener.RecordInterceptor`. On `intercept`, starts a `Timer.Sample`. On `success`/`failure`, stops the sample and increments the counter with `outcome=success/server_error`, `target=<topic>`. |
| `platform/lib-java/src/main/java/com/canary/platform/lib/observability/CanaryRestateMeter.java` | Static helper. `measure(handlerName, lane, ThrowingSupplier<T>)` — starts timer, runs body, classifies outcome, records counter. Used by 5.b to wrap handler bodies. |
| `platform/lib-java/src/main/java/com/canary/platform/lib/observability/LaneStateProbe.java` | Fabric8 watcher on `Service` resources in the configured namespace. Updates a `MultiGauge` named `canary_lane_active` with one row per `(substrate, service, lane)` based on whether the corresponding Service has a non-empty endpoint list. Modelled on `XCanaryPresenceWatcher`. |
| `platform/lib-java/src/main/java/com/canary/platform/lib/observability/CanaryHttpSpanFilter.java` | Implements `org.springframework.web.filter.OncePerRequestFilter`. After the chain runs, sets `canary.lane` and `canary.service` attributes on the active span (via OpenTelemetry's `Span.current()`). Ordered after `XCanaryRequestFilter` so `XCanaryContext` is populated. |
| `platform/lib-java/src/main/java/com/canary/platform/lib/observability/CanaryMetricsAutoConfiguration.java` | `@AutoConfiguration` that registers `CanaryMetrics`, `CanaryKafkaRecordInterceptor`, `LaneStateProbe`, `CanaryHttpSpanFilter`. Activates only when `MeterRegistry` bean is present (i.e. actuator is on the classpath, which is always true in this project). |
| `platform/lib-java/src/main/java/com/canary/platform/lib/observability/TracingAutoConfiguration.java` | `@AutoConfiguration` that ensures the OTLP exporter is configured. Registers a `MeterFilter` that strips `lane` from JVM-level meters (defensive — keeps cardinality bounded if any auto-instrumentation tries to inherit common tags). |
| `platform/lib-java/src/test/java/com/canary/platform/lib/observability/CanaryLaneTagTest.java` | Tests: `current()` returns `"stable"` when `XCanaryContext.isCanary()` is false, `"canary"` when true. |
| `platform/lib-java/src/test/java/com/canary/platform/lib/observability/CanaryMetricsTest.java` | Tests against a `SimpleMeterRegistry`: `recordHttp(target="POST /pay", outcome="success", durationMs=42)` → expected counter incremented + timer recorded with the expected tags. |
| `platform/lib-java/src/test/java/com/canary/platform/lib/observability/CanaryKafkaRecordInterceptorTest.java` | Tests using mocked `ConsumerRecord`: success path increments counter with `outcome="success"`; failure path with `outcome="server_error"`. |
| `platform/lib-java/src/test/java/com/canary/platform/lib/observability/CanaryRestateMeterTest.java` | Tests: returning normally → counter `success`, throwing → counter `server_error`, timer always recorded. |
| `platform/lib-java/src/test/java/com/canary/platform/lib/observability/LaneStateProbeTest.java` | Tests using a mock `KubernetesClient`: when watching a `Service` with subset selector matching canary pods, the gauge reports 1 for `lane=canary`. |
| `platform/lib-java/src/test/java/com/canary/platform/lib/observability/CanaryHttpSpanFilterTest.java` | Tests using mocked `Span`: filter calls `setAttribute("canary.lane", "canary")` when `XCanaryContext.isCanary()` is true. |
| `platform/lib-java/src/test/java/com/canary/platform/lib/observability/CanaryMetricsAutoConfigurationTest.java` | `ApplicationContextRunner`-based test: when context loads with a `SimpleMeterRegistry`, all four observability beans are present. |

### Modified

| Path | Change |
|---|---|
| `gradle/libs.versions.toml` | Add three library aliases: `micrometer-tracing-bridge-otel`, `opentelemetry-exporter-otlp`, `opentelemetry-spring-boot-starter`. |
| `platform/lib-java/build.gradle.kts` | Add the three new `api(...)` dependencies. |
| `platform/lib-java/src/main/java/com/canary/platform/lib/XCanaryRequestFilter.java` | Wrap chain with `Timer.Sample.start(registry)` and call `CanaryMetrics.recordHttp(...)` after status is known. Take `CanaryMetrics` via constructor. |
| `platform/lib-java/src/test/java/com/canary/platform/lib/XCanaryRequestFilterTest.java` | Update existing tests to provide a stub `CanaryMetrics`; assert that metric is recorded. |
| `platform/lib-java/src/main/java/com/canary/platform/lib/autoconfigure/XCanaryAutoConfiguration.java` | Update `xCanaryRequestFilter` bean to inject `CanaryMetrics`. Update `kafkaListenerContainerFactory` bean to also call `factory.setRecordInterceptor(canaryKafkaRecordInterceptor)`. Take `CanaryKafkaRecordInterceptor` as a parameter. |
| `platform/lib-java/src/test/java/com/canary/platform/lib/XCanaryAutoConfigurationTest.java` | Update wiring assertions to expect the new bean and the interceptor on the factory. |
| `platform/lib-java/src/main/resources/META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports` | Append `com.canary.platform.lib.observability.CanaryMetricsAutoConfiguration` and `com.canary.platform.lib.observability.TracingAutoConfiguration`. |
| `services/payment-service/src/main/resources/application.yml` | Add `prometheus` to actuator `exposure.include`. Add `management.tracing.sampling.probability: 1.0` and `management.otlp.tracing.endpoint`. |
| `services/audit-service/src/main/resources/application.yml` | Same as above. |
| `services/inventory-service/src/main/resources/application.yml` | Same as above. |
| `services/order-service/src/main/resources/application.yml` | Same as above. |
| `services/notification-service/src/main/resources/application.yml` | Same as above. |
| `deploy/helm/service-chart/templates/deployment.yaml` | Add `prometheus.io/scrape`, `prometheus.io/port`, `prometheus.io/path` annotations to pod template metadata (line 14). |

---

## Task 1 — Add dependencies to the version catalog

**Files:**
- Modify: `gradle/libs.versions.toml`

- [ ] **Step 1.1: Add version refs and library aliases**

Edit `gradle/libs.versions.toml`. Under `[versions]` add:

```toml
micrometerTracing = "1.5.4"
opentelemetry = "1.51.0"
opentelemetryInstrumentation = "2.21.1"
```

Under `[libraries]` add:

```toml
micrometer-tracing-bridge-otel    = { module = "io.micrometer:micrometer-tracing-bridge-otel",                  version.ref = "micrometerTracing" }
opentelemetry-exporter-otlp       = { module = "io.opentelemetry:opentelemetry-exporter-otlp",                  version.ref = "opentelemetry" }
opentelemetry-spring-boot-starter = { module = "io.opentelemetry.instrumentation:opentelemetry-spring-boot-starter", version.ref = "opentelemetryInstrumentation" }
```

Pin notes:
- Micrometer Tracing 1.5.x is the line that ships with Spring Boot 4.0.x (Boot 4.0.4 BOM imports `micrometer-tracing-bom:1.5.4`). Pinning explicitly avoids surprise upgrades.
- OpenTelemetry Java 1.51.0 is the latest stable as of 2026-Q1 and is BOM-compatible with the Micrometer 1.5.x bridge.
- OTel Spring Boot starter 2.21.1 is its own BOM line; verify no conflict with the existing `spring-boot-dependencies:4.0.4` BOM by running the tests in subsequent tasks.

- [ ] **Step 1.2: Verify the catalog parses**

Run: `./gradlew help`
Expected: `BUILD SUCCESSFUL`. Catalog parse errors surface here before any source change.

- [ ] **Step 1.3: Commit**

```bash
git add gradle/libs.versions.toml
git commit -m "build(deps): add micrometer-tracing-bridge-otel + otlp exporter + otel spring starter"
```

---

## Task 2 — Add dependencies to lib-java

**Files:**
- Modify: `platform/lib-java/build.gradle.kts`

- [ ] **Step 2.1: Add the three new `api` dependencies**

Edit `platform/lib-java/build.gradle.kts`. Inside the `dependencies { ... }` block, after the existing actuator + kafka + restate `api(...)` lines, add:

```kotlin
    api(libs.micrometer.tracing.bridge.otel)
    api(libs.opentelemetry.exporter.otlp)
    api(libs.opentelemetry.spring.boot.starter)
```

- [ ] **Step 2.2: Verify lib-java still compiles**

Run: `./gradlew :platform:lib-java:compileJava`
Expected: `BUILD SUCCESSFUL`. No source touched yet, so this just validates that the BOM resolves transitives without conflict.

- [ ] **Step 2.3: Verify lib-java tests still pass**

Run: `./gradlew :platform:lib-java:test`
Expected: `BUILD SUCCESSFUL`, all existing tests green. New deps must not break Phase 1–3 instrumentation.

- [ ] **Step 2.4: Commit**

```bash
git add platform/lib-java/build.gradle.kts
git commit -m "build(lib-java): pull in micrometer-tracing OTel bridge + OTel Spring starter"
```

---

## Task 3 — `CanaryLaneTag` helper

**Files:**
- Create: `platform/lib-java/src/main/java/com/canary/platform/lib/observability/CanaryLaneTag.java`
- Test: `platform/lib-java/src/test/java/com/canary/platform/lib/observability/CanaryLaneTagTest.java`

- [ ] **Step 3.1: Write the failing test**

Create `platform/lib-java/src/test/java/com/canary/platform/lib/observability/CanaryLaneTagTest.java`:

```java
package com.canary.platform.lib.observability;

import com.canary.platform.lib.XCanaryContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class CanaryLaneTagTest {

    @AfterEach
    void cleanContext() {
        XCanaryContext.clear();
    }

    @Test
    void currentReturnsStableWhenContextIsNotCanary() {
        XCanaryContext.set(false);
        assertThat(CanaryLaneTag.current()).isEqualTo("stable");
    }

    @Test
    void currentReturnsCanaryWhenContextIsCanary() {
        XCanaryContext.set(true);
        assertThat(CanaryLaneTag.current()).isEqualTo("canary");
    }

    @Test
    void currentDefaultsToStableWhenContextNotSet() {
        XCanaryContext.clear();
        assertThat(CanaryLaneTag.current()).isEqualTo("stable");
    }
}
```

- [ ] **Step 3.2: Run the test, verify it fails**

Run: `./gradlew :platform:lib-java:test --tests "com.canary.platform.lib.observability.CanaryLaneTagTest"`
Expected: `FAILED` with compile error: `package com.canary.platform.lib.observability does not exist` or `cannot find symbol CanaryLaneTag`.

- [ ] **Step 3.3: Implement**

Create `platform/lib-java/src/main/java/com/canary/platform/lib/observability/CanaryLaneTag.java`:

```java
package com.canary.platform.lib.observability;

import com.canary.platform.lib.XCanaryContext;

public final class CanaryLaneTag {

    public static final String STABLE = "stable";
    public static final String CANARY = "canary";

    private CanaryLaneTag() {}

    /** Returns the current lane tag value derived from {@link XCanaryContext}. */
    public static String current() {
        return XCanaryContext.isCanary() ? CANARY : STABLE;
    }
}
```

- [ ] **Step 3.4: Run the test, verify it passes**

Run: `./gradlew :platform:lib-java:test --tests "com.canary.platform.lib.observability.CanaryLaneTagTest"`
Expected: `BUILD SUCCESSFUL`, 3 tests passed.

- [ ] **Step 3.5: Commit**

```bash
git add platform/lib-java/src/main/java/com/canary/platform/lib/observability/CanaryLaneTag.java \
        platform/lib-java/src/test/java/com/canary/platform/lib/observability/CanaryLaneTagTest.java
git commit -m "feat(observability): CanaryLaneTag helper resolves lane string from XCanaryContext"
```

---

## Task 4 — `CanaryMetrics` central helper

**Files:**
- Create: `platform/lib-java/src/main/java/com/canary/platform/lib/observability/CanaryMetrics.java`
- Test: `platform/lib-java/src/test/java/com/canary/platform/lib/observability/CanaryMetricsTest.java`

- [ ] **Step 4.1: Write the failing test**

Create `platform/lib-java/src/test/java/com/canary/platform/lib/observability/CanaryMetricsTest.java`:

```java
package com.canary.platform.lib.observability;

import com.canary.platform.lib.XCanaryContext;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

class CanaryMetricsTest {

    private MeterRegistry registry;
    private CanaryMetrics metrics;

    @BeforeEach
    void setUp() {
        registry = new SimpleMeterRegistry();
        metrics = new CanaryMetrics(registry, "payment");
    }

    @AfterEach
    void tearDown() {
        XCanaryContext.clear();
    }

    @Test
    void recordHttpIncrementsCounterWithExpectedTags() {
        XCanaryContext.set(false);
        metrics.recordHttp("POST /pay", "success", Duration.ofMillis(42));

        Counter c = registry.find("canary_request_total")
                .tags("substrate", "http", "service", "payment", "lane", "stable",
                      "outcome", "success", "target", "POST /pay")
                .counter();
        assertThat(c).isNotNull();
        assertThat(c.count()).isEqualTo(1.0);
    }

    @Test
    void recordHttpRecordsTimerWithExpectedTags() {
        XCanaryContext.set(true);
        metrics.recordHttp("GET /healthz", "success", Duration.ofMillis(7));

        Timer t = registry.find("canary_request_duration_seconds")
                .tags("substrate", "http", "service", "payment", "lane", "canary",
                      "target", "GET /healthz")
                .timer();
        assertThat(t).isNotNull();
        assertThat(t.count()).isEqualTo(1L);
        assertThat(t.totalTime(java.util.concurrent.TimeUnit.MILLISECONDS)).isEqualTo(7.0);
    }

    @Test
    void recordKafkaUsesKafkaSubstrateAndTopicTarget() {
        XCanaryContext.set(true);
        metrics.recordKafka("payments.charged", "success", Duration.ofMillis(100));

        Counter c = registry.find("canary_request_total")
                .tags("substrate", "kafka", "service", "payment", "lane", "canary",
                      "outcome", "success", "target", "payments.charged")
                .counter();
        assertThat(c).isNotNull();
        assertThat(c.count()).isEqualTo(1.0);
    }

    @Test
    void recordRestateUsesRestateSubstrateAndHandlerTarget() {
        XCanaryContext.set(false);
        metrics.recordRestate("CheckoutSagaStable.run", "server_error", Duration.ofMillis(250));

        Counter c = registry.find("canary_request_total")
                .tags("substrate", "restate", "service", "payment", "lane", "stable",
                      "outcome", "server_error", "target", "CheckoutSagaStable.run")
                .counter();
        assertThat(c).isNotNull();
        assertThat(c.count()).isEqualTo(1.0);
    }

    @Test
    void recordShadowMismatchIncrementsByService() {
        metrics.recordShadowMismatch("totalCents");

        Counter c = registry.find("canary_shadow_mismatch_total")
                .tags("service", "payment", "field", "totalCents")
                .counter();
        assertThat(c).isNotNull();
        assertThat(c.count()).isEqualTo(1.0);
    }
}
```

- [ ] **Step 4.2: Run the test, verify it fails**

Run: `./gradlew :platform:lib-java:test --tests "com.canary.platform.lib.observability.CanaryMetricsTest"`
Expected: `FAILED` with `cannot find symbol CanaryMetrics`.

- [ ] **Step 4.3: Implement**

Create `platform/lib-java/src/main/java/com/canary/platform/lib/observability/CanaryMetrics.java`:

```java
package com.canary.platform.lib.observability;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;

import java.time.Duration;

public class CanaryMetrics {

    private static final String COUNTER_NAME = "canary_request_total";
    private static final String TIMER_NAME = "canary_request_duration_seconds";
    private static final String SHADOW_NAME = "canary_shadow_mismatch_total";

    private final MeterRegistry registry;
    private final String serviceName;

    public CanaryMetrics(MeterRegistry registry, String serviceName) {
        this.registry = registry;
        this.serviceName = serviceName;
    }

    public void recordHttp(String target, String outcome, Duration duration) {
        record("http", target, outcome, duration);
    }

    public void recordKafka(String target, String outcome, Duration duration) {
        record("kafka", target, outcome, duration);
    }

    public void recordRestate(String target, String outcome, Duration duration) {
        record("restate", target, outcome, duration);
    }

    public void recordShadowMismatch(String field) {
        Counter.builder(SHADOW_NAME)
                .tags("service", serviceName, "field", field)
                .register(registry)
                .increment();
    }

    private void record(String substrate, String target, String outcome, Duration duration) {
        String lane = CanaryLaneTag.current();
        Tags counterTags = Tags.of(
                "substrate", substrate,
                "service", serviceName,
                "lane", lane,
                "outcome", outcome,
                "target", target);
        Counter.builder(COUNTER_NAME).tags(counterTags).register(registry).increment();

        Tags timerTags = Tags.of(
                "substrate", substrate,
                "service", serviceName,
                "lane", lane,
                "target", target);
        Timer.builder(TIMER_NAME)
                .tags(timerTags)
                .publishPercentileHistogram()
                .register(registry)
                .record(duration);
    }
}
```

- [ ] **Step 4.4: Run the test, verify it passes**

Run: `./gradlew :platform:lib-java:test --tests "com.canary.platform.lib.observability.CanaryMetricsTest"`
Expected: `BUILD SUCCESSFUL`, 5 tests passed.

- [ ] **Step 4.5: Commit**

```bash
git add platform/lib-java/src/main/java/com/canary/platform/lib/observability/CanaryMetrics.java \
        platform/lib-java/src/test/java/com/canary/platform/lib/observability/CanaryMetricsTest.java
git commit -m "feat(observability): CanaryMetrics central helper for the four canary-aware meters"
```

---

## Task 5 — Augment `XCanaryRequestFilter` to emit HTTP metrics

**Files:**
- Modify: `platform/lib-java/src/main/java/com/canary/platform/lib/XCanaryRequestFilter.java`
- Modify: `platform/lib-java/src/test/java/com/canary/platform/lib/XCanaryRequestFilterTest.java`

- [ ] **Step 5.1: Update existing test to assert metric emission**

Read the existing `XCanaryRequestFilterTest.java` (already in repo). Add a new test alongside it. The existing tests construct `new XCanaryRequestFilter()` with no args; that constructor will go away in Step 5.3. Update those tests to use `new XCanaryRequestFilter(metrics)` with a `CanaryMetrics` constructed from `SimpleMeterRegistry`.

Add this new test method to the existing class:

```java
@Test
void doFilterRecordsHttpMetricForCanaryRequest() throws Exception {
    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    CanaryMetrics metrics = new CanaryMetrics(registry, "payment");
    XCanaryRequestFilter filter = new XCanaryRequestFilter(metrics);

    MockHttpServletRequest req = new MockHttpServletRequest("GET", "/healthz");
    req.addHeader(XCanaryConstants.HEADER_NAME, XCanaryConstants.TRUE_VALUE);
    MockHttpServletResponse resp = new MockHttpServletResponse();
    resp.setStatus(200);

    filter.doFilter(req, resp, (r, s) -> {});

    Counter c = registry.find("canary_request_total")
            .tags("substrate", "http", "service", "payment", "lane", "canary", "outcome", "success")
            .counter();
    assertThat(c).isNotNull();
    assertThat(c.count()).isEqualTo(1.0);
}

@Test
void doFilterTagsClientErrorOn4xx() throws Exception {
    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    CanaryMetrics metrics = new CanaryMetrics(registry, "payment");
    XCanaryRequestFilter filter = new XCanaryRequestFilter(metrics);

    MockHttpServletRequest req = new MockHttpServletRequest("POST", "/pay");
    MockHttpServletResponse resp = new MockHttpServletResponse();
    filter.doFilter(req, resp, (r, s) -> ((MockHttpServletResponse) s).setStatus(404));

    Counter c = registry.find("canary_request_total").tag("outcome", "client_error").counter();
    assertThat(c).isNotNull();
    assertThat(c.count()).isEqualTo(1.0);
}

@Test
void doFilterTagsServerErrorOn5xx() throws Exception {
    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    CanaryMetrics metrics = new CanaryMetrics(registry, "payment");
    XCanaryRequestFilter filter = new XCanaryRequestFilter(metrics);

    MockHttpServletRequest req = new MockHttpServletRequest("POST", "/pay");
    MockHttpServletResponse resp = new MockHttpServletResponse();
    filter.doFilter(req, resp, (r, s) -> ((MockHttpServletResponse) s).setStatus(503));

    Counter c = registry.find("canary_request_total").tag("outcome", "server_error").counter();
    assertThat(c).isNotNull();
    assertThat(c.count()).isEqualTo(1.0);
}
```

Add the necessary imports (`SimpleMeterRegistry`, `CanaryMetrics`, `Counter`, `MockHttpServletRequest/Response`).

- [ ] **Step 5.2: Run the tests, verify they fail**

Run: `./gradlew :platform:lib-java:test --tests "com.canary.platform.lib.XCanaryRequestFilterTest"`
Expected: `FAILED`. Either compile error (constructor signature) or the new tests fail because no counter is registered.

- [ ] **Step 5.3: Implement**

Replace `platform/lib-java/src/main/java/com/canary/platform/lib/XCanaryRequestFilter.java` with:

```java
package com.canary.platform.lib;

import com.canary.platform.lib.observability.CanaryMetrics;
import jakarta.servlet.Filter;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletRequest;
import jakarta.servlet.ServletResponse;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.core.Ordered;

import java.io.IOException;
import java.time.Duration;

public class XCanaryRequestFilter implements Filter, Ordered {

    private final CanaryMetrics metrics;

    public XCanaryRequestFilter(CanaryMetrics metrics) {
        this.metrics = metrics;
    }

    @Override
    public int getOrder() {
        return Ordered.HIGHEST_PRECEDENCE + 100;
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
            throws IOException, ServletException {
        boolean canary = false;
        String target = "unknown";
        if (request instanceof HttpServletRequest http) {
            String header = http.getHeader(XCanaryConstants.HEADER_NAME);
            canary = XCanaryConstants.TRUE_VALUE.equals(header);
            target = http.getMethod() + " " + http.getRequestURI();
        }
        boolean prior = XCanaryContext.isCanary();
        XCanaryContext.set(canary);
        long startNanos = System.nanoTime();
        try {
            chain.doFilter(request, response);
        } finally {
            Duration elapsed = Duration.ofNanos(System.nanoTime() - startNanos);
            String outcome = classify(response);
            metrics.recordHttp(target, outcome, elapsed);

            XCanaryContext.set(prior);
            if (!prior) {
                XCanaryContext.clear();
            }
        }
    }

    private static String classify(ServletResponse response) {
        if (response instanceof HttpServletResponse http) {
            int status = http.getStatus();
            if (status >= 500) return "server_error";
            if (status >= 400) return "client_error";
        }
        return "success";
    }
}
```

- [ ] **Step 5.4: Run the tests, verify they pass**

Run: `./gradlew :platform:lib-java:test --tests "com.canary.platform.lib.XCanaryRequestFilterTest"`
Expected: `BUILD SUCCESSFUL`. Both new and pre-existing tests pass.

- [ ] **Step 5.5: Commit**

```bash
git add platform/lib-java/src/main/java/com/canary/platform/lib/XCanaryRequestFilter.java \
        platform/lib-java/src/test/java/com/canary/platform/lib/XCanaryRequestFilterTest.java
git commit -m "feat(observability): emit canary_request_total + duration on HTTP filter"
```

---

## Task 6 — `CanaryKafkaRecordInterceptor`

**Files:**
- Create: `platform/lib-java/src/main/java/com/canary/platform/lib/observability/CanaryKafkaRecordInterceptor.java`
- Test: `platform/lib-java/src/test/java/com/canary/platform/lib/observability/CanaryKafkaRecordInterceptorTest.java`

- [ ] **Step 6.1: Write the failing test**

Create `CanaryKafkaRecordInterceptorTest.java`:

```java
package com.canary.platform.lib.observability;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.assertj.core.api.Assertions.assertThat;

class CanaryKafkaRecordInterceptorTest {

    private SimpleMeterRegistry registry;
    private CanaryMetrics metrics;
    private CanaryKafkaRecordInterceptor<Object, Object> interceptor;
    private Consumer<Object, Object> consumer;

    @BeforeEach
    void setUp() {
        registry = new SimpleMeterRegistry();
        metrics = new CanaryMetrics(registry, "audit");
        interceptor = new CanaryKafkaRecordInterceptor<>(metrics);
        consumer = Mockito.mock(Consumer.class);
    }

    @Test
    void successPathIncrementsCounterWithSuccessOutcome() {
        ConsumerRecord<Object, Object> rec = new ConsumerRecord<>("payments.charged", 0, 0L, "k", "v");
        interceptor.intercept(rec, consumer);
        interceptor.success(rec, consumer);

        Counter c = registry.find("canary_request_total")
                .tags("substrate", "kafka", "service", "audit", "outcome", "success", "target", "payments.charged")
                .counter();
        assertThat(c).isNotNull();
        assertThat(c.count()).isEqualTo(1.0);
    }

    @Test
    void failurePathIncrementsCounterWithServerErrorOutcome() {
        ConsumerRecord<Object, Object> rec = new ConsumerRecord<>("payments.charged", 0, 0L, "k", "v");
        interceptor.intercept(rec, consumer);
        interceptor.failure(rec, new RuntimeException("boom"), consumer);

        Counter c = registry.find("canary_request_total")
                .tags("substrate", "kafka", "service", "audit", "outcome", "server_error", "target", "payments.charged")
                .counter();
        assertThat(c).isNotNull();
        assertThat(c.count()).isEqualTo(1.0);
    }

    @Test
    void interceptReturnsRecordUnchanged() {
        ConsumerRecord<Object, Object> rec = new ConsumerRecord<>("t", 0, 0L, "k", "v");
        ConsumerRecord<Object, Object> result = interceptor.intercept(rec, consumer);
        assertThat(result).isSameAs(rec);
    }
}
```

- [ ] **Step 6.2: Run the test, verify it fails**

Run: `./gradlew :platform:lib-java:test --tests "com.canary.platform.lib.observability.CanaryKafkaRecordInterceptorTest"`
Expected: `FAILED` — `cannot find symbol CanaryKafkaRecordInterceptor`.

- [ ] **Step 6.3: Implement**

Create `platform/lib-java/src/main/java/com/canary/platform/lib/observability/CanaryKafkaRecordInterceptor.java`:

```java
package com.canary.platform.lib.observability;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.listener.RecordInterceptor;

import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class CanaryKafkaRecordInterceptor<K, V> implements RecordInterceptor<K, V> {

    private final CanaryMetrics metrics;
    // start-time per record, identified by topic+partition+offset (handler chains may interleave)
    private final ConcurrentMap<String, Long> starts = new ConcurrentHashMap<>();

    public CanaryKafkaRecordInterceptor(CanaryMetrics metrics) {
        this.metrics = metrics;
    }

    @Override
    public ConsumerRecord<K, V> intercept(ConsumerRecord<K, V> record, Consumer<K, V> consumer) {
        starts.put(key(record), System.nanoTime());
        return record;
    }

    @Override
    public void success(ConsumerRecord<K, V> record, Consumer<K, V> consumer) {
        record(record, "success");
    }

    @Override
    public void failure(ConsumerRecord<K, V> record, Exception exception, Consumer<K, V> consumer) {
        record(record, "server_error");
    }

    private void record(ConsumerRecord<K, V> r, String outcome) {
        Long start = starts.remove(key(r));
        Duration elapsed = (start == null)
                ? Duration.ZERO
                : Duration.ofNanos(System.nanoTime() - start);
        metrics.recordKafka(r.topic(), outcome, elapsed);
    }

    private static String key(ConsumerRecord<?, ?> r) {
        return r.topic() + ":" + r.partition() + ":" + r.offset();
    }
}
```

- [ ] **Step 6.4: Run the test, verify it passes**

Run: `./gradlew :platform:lib-java:test --tests "com.canary.platform.lib.observability.CanaryKafkaRecordInterceptorTest"`
Expected: `BUILD SUCCESSFUL`, 3 tests passed.

- [ ] **Step 6.5: Commit**

```bash
git add platform/lib-java/src/main/java/com/canary/platform/lib/observability/CanaryKafkaRecordInterceptor.java \
        platform/lib-java/src/test/java/com/canary/platform/lib/observability/CanaryKafkaRecordInterceptorTest.java
git commit -m "feat(observability): CanaryKafkaRecordInterceptor times Kafka consumer records per-lane"
```

---

## Task 7 — `CanaryRestateMeter` helper (used by 5.b)

**Files:**
- Create: `platform/lib-java/src/main/java/com/canary/platform/lib/observability/CanaryRestateMeter.java`
- Test: `platform/lib-java/src/test/java/com/canary/platform/lib/observability/CanaryRestateMeterTest.java`

- [ ] **Step 7.1: Write the failing test**

Create `CanaryRestateMeterTest.java`:

```java
package com.canary.platform.lib.observability;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CanaryRestateMeterTest {

    private SimpleMeterRegistry registry;
    private CanaryMetrics metrics;
    private CanaryRestateMeter meter;

    @BeforeEach
    void setUp() {
        registry = new SimpleMeterRegistry();
        metrics = new CanaryMetrics(registry, "order");
        meter = new CanaryRestateMeter(metrics);
    }

    @Test
    void measureReturnsValueAndRecordsSuccess() throws Exception {
        String result = meter.measure("CheckoutSagaStable.run", () -> "ok");
        assertThat(result).isEqualTo("ok");

        Counter c = registry.find("canary_request_total")
                .tags("substrate", "restate", "outcome", "success", "target", "CheckoutSagaStable.run")
                .counter();
        assertThat(c).isNotNull();
        assertThat(c.count()).isEqualTo(1.0);
    }

    @Test
    void measureRecordsServerErrorWhenBodyThrows() {
        assertThatThrownBy(() -> meter.measure("CheckoutSagaStable.run", () -> {
            throw new RuntimeException("kaboom");
        })).isInstanceOf(RuntimeException.class).hasMessage("kaboom");

        Counter c = registry.find("canary_request_total")
                .tags("substrate", "restate", "outcome", "server_error", "target", "CheckoutSagaStable.run")
                .counter();
        assertThat(c).isNotNull();
        assertThat(c.count()).isEqualTo(1.0);
    }

    @Test
    void measureAlwaysRecordsTimer() throws Exception {
        meter.measure("PaymentVOStable.charge", () -> 1);

        Timer t = registry.find("canary_request_duration_seconds")
                .tags("substrate", "restate", "target", "PaymentVOStable.charge")
                .timer();
        assertThat(t).isNotNull();
        assertThat(t.count()).isEqualTo(1L);
    }
}
```

- [ ] **Step 7.2: Run the test, verify it fails**

Run: `./gradlew :platform:lib-java:test --tests "com.canary.platform.lib.observability.CanaryRestateMeterTest"`
Expected: `FAILED` — `cannot find symbol CanaryRestateMeter`.

- [ ] **Step 7.3: Implement**

Create `platform/lib-java/src/main/java/com/canary/platform/lib/observability/CanaryRestateMeter.java`:

```java
package com.canary.platform.lib.observability;

import java.time.Duration;

public class CanaryRestateMeter {

    @FunctionalInterface
    public interface ThrowingSupplier<T> {
        T get() throws Exception;
    }

    private final CanaryMetrics metrics;

    public CanaryRestateMeter(CanaryMetrics metrics) {
        this.metrics = metrics;
    }

    public <T> T measure(String handlerName, ThrowingSupplier<T> body) throws Exception {
        long start = System.nanoTime();
        try {
            T result = body.get();
            metrics.recordRestate(handlerName, "success", Duration.ofNanos(System.nanoTime() - start));
            return result;
        } catch (Exception e) {
            metrics.recordRestate(handlerName, "server_error", Duration.ofNanos(System.nanoTime() - start));
            throw e;
        }
    }
}
```

- [ ] **Step 7.4: Run the test, verify it passes**

Run: `./gradlew :platform:lib-java:test --tests "com.canary.platform.lib.observability.CanaryRestateMeterTest"`
Expected: `BUILD SUCCESSFUL`, 3 tests passed.

- [ ] **Step 7.5: Commit**

```bash
git add platform/lib-java/src/main/java/com/canary/platform/lib/observability/CanaryRestateMeter.java \
        platform/lib-java/src/test/java/com/canary/platform/lib/observability/CanaryRestateMeterTest.java
git commit -m "feat(observability): CanaryRestateMeter helper for handler-level metric emission"
```

---

## Task 8 — `LaneStateProbe` k8s watcher → `canary_lane_active` gauge

**Files:**
- Create: `platform/lib-java/src/main/java/com/canary/platform/lib/observability/LaneStateProbe.java`
- Test: `platform/lib-java/src/test/java/com/canary/platform/lib/observability/LaneStateProbeTest.java`

Background: `XCanaryPresenceWatcher` already watches `Service` resources via Fabric8 to detect canary readiness. `LaneStateProbe` reuses the same approach but produces a Micrometer gauge instead of a boolean.

Design note: to avoid pulling in `kubernetes-server-mock` + `awaitility` (neither is currently a test dep — the existing `XCanaryPresenceWatcherTest` is purely Mockito-based), `LaneStateProbe` exposes a small public `setLaneActive(lane, isActive)` method that the watcher's event handler calls. Tests exercise that method + assert gauge state, then verify watcher wiring via mocks. Same pattern as `XCanaryPresenceWatcherTest`.

- [ ] **Step 8.1: Write the failing test**

Create `LaneStateProbeTest.java`:

```java
package com.canary.platform.lib.observability;

import io.fabric8.kubernetes.api.model.EndpointAddressBuilder;
import io.fabric8.kubernetes.api.model.EndpointSubsetBuilder;
import io.fabric8.kubernetes.api.model.Endpoints;
import io.fabric8.kubernetes.api.model.EndpointsBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class LaneStateProbeTest {

    private SimpleMeterRegistry registry;
    private KubernetesClient client;

    @BeforeEach
    @SuppressWarnings({"unchecked", "rawtypes"})
    void setUp() {
        registry = new SimpleMeterRegistry();
        client = mock(KubernetesClient.class);
        // Stub the endpoints DSL chain so probe.start() does not NPE
        MixedOperation endpointsDsl = mock(MixedOperation.class);
        NonNamespaceOperation namespaceOp = mock(NonNamespaceOperation.class);
        Resource resource = mock(Resource.class);
        when(client.endpoints()).thenReturn(endpointsDsl);
        when(endpointsDsl.inNamespace(any())).thenReturn(namespaceOp);
        when(namespaceOp.withName(any())).thenReturn(resource);
        when(resource.get()).thenReturn(null);   // initial state: no endpoints
        when(resource.watch(any())).thenReturn(mock(io.fabric8.kubernetes.client.Watch.class));
    }

    @Test
    void startRegistersGaugesForBothLanes() {
        LaneStateProbe probe = new LaneStateProbe(client, registry, "services", "payment");
        probe.start();

        Gauge stable = registry.find("canary_lane_active")
                .tags("service", "payment", "lane", "stable").gauge();
        Gauge canary = registry.find("canary_lane_active")
                .tags("service", "payment", "lane", "canary").gauge();
        assertThat(stable).isNotNull();
        assertThat(canary).isNotNull();
        assertThat(stable.value()).isEqualTo(0.0);
        assertThat(canary.value()).isEqualTo(0.0);

        probe.close();
    }

    @Test
    void setLaneActiveTogglesGaugeValue() {
        LaneStateProbe probe = new LaneStateProbe(client, registry, "services", "payment");
        probe.start();

        probe.setLaneActive("canary", true);

        Gauge canary = registry.find("canary_lane_active")
                .tags("service", "payment", "lane", "canary").gauge();
        assertThat(canary.value()).isEqualTo(1.0);

        probe.setLaneActive("canary", false);
        assertThat(canary.value()).isEqualTo(0.0);

        probe.close();
    }

    @Test
    void hasAddressesReturnsTrueForPopulatedEndpoints() {
        Endpoints e = new EndpointsBuilder()
                .withNewMetadata().withName("svc").endMetadata()
                .addToSubsets(new EndpointSubsetBuilder()
                        .addToAddresses(new EndpointAddressBuilder().withIp("10.0.0.1").build())
                        .build())
                .build();
        assertThat(LaneStateProbe.hasAddresses(e)).isTrue();
    }

    @Test
    void hasAddressesReturnsFalseForNullOrEmpty() {
        assertThat(LaneStateProbe.hasAddresses(null)).isFalse();
        Endpoints e = new EndpointsBuilder().withNewMetadata().withName("svc").endMetadata().build();
        assertThat(LaneStateProbe.hasAddresses(e)).isFalse();
    }
}
```

- [ ] **Step 8.2: Run the test, verify it fails**

Run: `./gradlew :platform:lib-java:test --tests "com.canary.platform.lib.observability.LaneStateProbeTest"`
Expected: `FAILED` — `cannot find symbol LaneStateProbe`.

- [ ] **Step 8.3: Implement**

Create `platform/lib-java/src/main/java/com/canary/platform/lib/observability/LaneStateProbe.java`:

```java
package com.canary.platform.lib.observability;

import io.fabric8.kubernetes.api.model.Endpoints;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.Watch;
import io.fabric8.kubernetes.client.Watcher;
import io.fabric8.kubernetes.client.WatcherException;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class LaneStateProbe implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(LaneStateProbe.class);

    private final KubernetesClient client;
    private final MeterRegistry registry;
    private final String namespace;
    private final String serviceName;

    private final Map<String, AtomicInteger> laneState = new ConcurrentHashMap<>();
    private Watch stableWatch;
    private Watch canaryWatch;

    public LaneStateProbe(KubernetesClient client, MeterRegistry registry,
                          String namespace, String serviceName) {
        this.client = client;
        this.registry = registry;
        this.namespace = namespace;
        this.serviceName = serviceName;
    }

    public void start() {
        registerGauge("stable");
        registerGauge("canary");
        stableWatch = watchEndpoints(serviceName + "-stable", "stable");
        canaryWatch = watchEndpoints(serviceName + "-canary", "canary");
    }

    /** Visible for testing: directly toggle a lane's active value. */
    public void setLaneActive(String lane, boolean active) {
        laneState.computeIfAbsent(lane, l -> new AtomicInteger(0)).set(active ? 1 : 0);
    }

    private void registerGauge(String lane) {
        AtomicInteger holder = laneState.computeIfAbsent(lane, l -> new AtomicInteger(0));
        Gauge.builder("canary_lane_active", holder, AtomicInteger::doubleValue)
                .tags("service", serviceName, "lane", lane)
                .strongReference(true)
                .register(registry);
    }

    private Watch watchEndpoints(String endpointsName, String lane) {
        Endpoints e = client.endpoints().inNamespace(namespace).withName(endpointsName).get();
        setLaneActive(lane, hasAddresses(e));

        return client.endpoints().inNamespace(namespace).withName(endpointsName)
                .watch(new Watcher<>() {
                    @Override
                    public void eventReceived(Action action, Endpoints resource) {
                        boolean active = (action != Action.DELETED) && hasAddresses(resource);
                        setLaneActive(lane, active);
                    }

                    @Override
                    public void onClose(WatcherException cause) {
                        if (cause != null) {
                            log.warn("Endpoints watch for {} closed with error", endpointsName, cause);
                        }
                    }
                });
    }

    static boolean hasAddresses(Endpoints e) {
        if (e == null || e.getSubsets() == null) return false;
        return e.getSubsets().stream()
                .anyMatch(s -> s.getAddresses() != null && !s.getAddresses().isEmpty());
    }

    @Override
    public void close() {
        if (stableWatch != null) stableWatch.close();
        if (canaryWatch != null) canaryWatch.close();
    }
}
```

- [ ] **Step 8.4: Run the test, verify it passes**

Run: `./gradlew :platform:lib-java:test --tests "com.canary.platform.lib.observability.LaneStateProbeTest"`
Expected: `BUILD SUCCESSFUL`, 4 tests passed.

- [ ] **Step 8.5: Commit**

```bash
git add platform/lib-java/src/main/java/com/canary/platform/lib/observability/LaneStateProbe.java \
        platform/lib-java/src/test/java/com/canary/platform/lib/observability/LaneStateProbeTest.java
git commit -m "feat(observability): LaneStateProbe emits canary_lane_active gauge from k8s endpoints watch"
```

---

## Task 9 — `CanaryHttpSpanFilter` adds `canary.lane` span attribute

**Files:**
- Create: `platform/lib-java/src/main/java/com/canary/platform/lib/observability/CanaryHttpSpanFilter.java`
- Test: `platform/lib-java/src/test/java/com/canary/platform/lib/observability/CanaryHttpSpanFilterTest.java`

- [ ] **Step 9.1: Write the failing test**

Create `CanaryHttpSpanFilterTest.java`:

```java
package com.canary.platform.lib.observability;

import com.canary.platform.lib.XCanaryContext;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.mock.web.MockFilterChain;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

import static org.mockito.Mockito.verify;

class CanaryHttpSpanFilterTest {

    @AfterEach
    void cleanContext() {
        XCanaryContext.clear();
    }

    @Test
    void canaryRequestSetsLaneAttributeOnActiveSpan() throws Exception {
        Span mockSpan = Mockito.mock(Span.class);
        Mockito.when(mockSpan.setAttribute(Mockito.anyString(), Mockito.anyString())).thenReturn(mockSpan);

        try (Scope ignored = Context.current().with(mockSpan).makeCurrent()) {
            XCanaryContext.set(true);
            CanaryHttpSpanFilter filter = new CanaryHttpSpanFilter("payment");
            filter.doFilter(new MockHttpServletRequest("GET", "/x"),
                            new MockHttpServletResponse(), new MockFilterChain());
        }

        verify(mockSpan).setAttribute("canary.lane", "canary");
        verify(mockSpan).setAttribute("canary.service", "payment");
    }

    @Test
    void stableRequestSetsStableLane() throws Exception {
        Span mockSpan = Mockito.mock(Span.class);
        Mockito.when(mockSpan.setAttribute(Mockito.anyString(), Mockito.anyString())).thenReturn(mockSpan);

        try (Scope ignored = Context.current().with(mockSpan).makeCurrent()) {
            XCanaryContext.set(false);
            CanaryHttpSpanFilter filter = new CanaryHttpSpanFilter("payment");
            filter.doFilter(new MockHttpServletRequest("GET", "/x"),
                            new MockHttpServletResponse(), new MockFilterChain());
        }

        verify(mockSpan).setAttribute("canary.lane", "stable");
    }
}
```

- [ ] **Step 9.2: Run the test, verify it fails**

Run: `./gradlew :platform:lib-java:test --tests "com.canary.platform.lib.observability.CanaryHttpSpanFilterTest"`
Expected: `FAILED` — `cannot find symbol CanaryHttpSpanFilter`.

- [ ] **Step 9.3: Implement**

Create `platform/lib-java/src/main/java/com/canary/platform/lib/observability/CanaryHttpSpanFilter.java`:

```java
package com.canary.platform.lib.observability;

import io.opentelemetry.api.trace.Span;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.core.Ordered;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;

public class CanaryHttpSpanFilter extends OncePerRequestFilter implements Ordered {

    private final String serviceName;

    public CanaryHttpSpanFilter(String serviceName) {
        this.serviceName = serviceName;
    }

    @Override
    public int getOrder() {
        // After XCanaryRequestFilter (HIGHEST_PRECEDENCE+100) so XCanaryContext is set.
        return Ordered.HIGHEST_PRECEDENCE + 200;
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
            throws ServletException, IOException {
        Span span = Span.current();
        span.setAttribute("canary.lane", CanaryLaneTag.current());
        span.setAttribute("canary.service", serviceName);
        chain.doFilter(request, response);
    }
}
```

- [ ] **Step 9.4: Run the test, verify it passes**

Run: `./gradlew :platform:lib-java:test --tests "com.canary.platform.lib.observability.CanaryHttpSpanFilterTest"`
Expected: `BUILD SUCCESSFUL`, 2 tests passed.

- [ ] **Step 9.5: Commit**

```bash
git add platform/lib-java/src/main/java/com/canary/platform/lib/observability/CanaryHttpSpanFilter.java \
        platform/lib-java/src/test/java/com/canary/platform/lib/observability/CanaryHttpSpanFilterTest.java
git commit -m "feat(observability): CanaryHttpSpanFilter tags active span with canary.lane + canary.service"
```

---

## Task 10 — `CanaryMetricsAutoConfiguration`

**Files:**
- Create: `platform/lib-java/src/main/java/com/canary/platform/lib/observability/CanaryMetricsAutoConfiguration.java`
- Test: `platform/lib-java/src/test/java/com/canary/platform/lib/observability/CanaryMetricsAutoConfigurationTest.java`

- [ ] **Step 10.1: Write the failing test**

Create `CanaryMetricsAutoConfigurationTest.java`:

```java
package com.canary.platform.lib.observability;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import static org.assertj.core.api.Assertions.assertThat;

class CanaryMetricsAutoConfigurationTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(CanaryMetricsAutoConfiguration.class))
            .withUserConfiguration(TestSupport.class)
            .withPropertyValues("canary.service-name=payment");

    @Test
    void registersAllObservabilityBeans() {
        runner.run(ctx -> {
            assertThat(ctx).hasSingleBean(CanaryMetrics.class);
            assertThat(ctx).hasSingleBean(CanaryKafkaRecordInterceptor.class);
            assertThat(ctx).hasSingleBean(CanaryRestateMeter.class);
            assertThat(ctx).hasSingleBean(CanaryHttpSpanFilter.class);
            assertThat(ctx).hasSingleBean(LaneStateProbe.class);
        });
    }

    @Configuration
    static class TestSupport {
        @Bean MeterRegistry meterRegistry() { return new SimpleMeterRegistry(); }
        @Bean KubernetesClient kubernetesClient() { return Mockito.mock(KubernetesClient.class); }
    }
}
```

- [ ] **Step 10.2: Run the test, verify it fails**

Run: `./gradlew :platform:lib-java:test --tests "com.canary.platform.lib.observability.CanaryMetricsAutoConfigurationTest"`
Expected: `FAILED` — `cannot find symbol CanaryMetricsAutoConfiguration`.

- [ ] **Step 10.3: Implement**

Create `platform/lib-java/src/main/java/com/canary/platform/lib/observability/CanaryMetricsAutoConfiguration.java`:

```java
package com.canary.platform.lib.observability;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;

@AutoConfiguration
@ConditionalOnBean(MeterRegistry.class)
public class CanaryMetricsAutoConfiguration {

    @Bean
    public CanaryMetrics canaryMetrics(
            MeterRegistry registry,
            @Value("${canary.service-name:${SERVICE_NAME:unknown}}") String serviceName) {
        return new CanaryMetrics(registry, serviceName);
    }

    @Bean
    public CanaryKafkaRecordInterceptor<Object, Object> canaryKafkaRecordInterceptor(CanaryMetrics metrics) {
        return new CanaryKafkaRecordInterceptor<>(metrics);
    }

    @Bean
    public CanaryRestateMeter canaryRestateMeter(CanaryMetrics metrics) {
        return new CanaryRestateMeter(metrics);
    }

    @Bean
    public CanaryHttpSpanFilter canaryHttpSpanFilter(
            @Value("${canary.service-name:${SERVICE_NAME:unknown}}") String serviceName) {
        return new CanaryHttpSpanFilter(serviceName);
    }

    @Bean
    @ConditionalOnMissingBean
    public KubernetesClient kubernetesClient() {
        return new KubernetesClientBuilder().build();
    }

    @Bean(destroyMethod = "close")
    public LaneStateProbe laneStateProbe(
            KubernetesClient client,
            MeterRegistry registry,
            @Value("${canary.namespace:${POD_NAMESPACE:services}}") String namespace,
            @Value("${canary.service-name:${SERVICE_NAME:unknown}}") String serviceName) {
        LaneStateProbe probe = new LaneStateProbe(client, registry, namespace, serviceName);
        probe.start();
        return probe;
    }
}
```

- [ ] **Step 10.4: Run the test, verify it passes**

Run: `./gradlew :platform:lib-java:test --tests "com.canary.platform.lib.observability.CanaryMetricsAutoConfigurationTest"`
Expected: `BUILD SUCCESSFUL`. All five beans present.

- [ ] **Step 10.5: Commit**

```bash
git add platform/lib-java/src/main/java/com/canary/platform/lib/observability/CanaryMetricsAutoConfiguration.java \
        platform/lib-java/src/test/java/com/canary/platform/lib/observability/CanaryMetricsAutoConfigurationTest.java
git commit -m "feat(observability): CanaryMetricsAutoConfiguration wires the five beans"
```

---

## Task 11 — Wire `CanaryKafkaRecordInterceptor` into `XCanaryAutoConfiguration`

**Files:**
- Modify: `platform/lib-java/src/main/java/com/canary/platform/lib/autoconfigure/XCanaryAutoConfiguration.java`
- Modify: `platform/lib-java/src/test/java/com/canary/platform/lib/XCanaryAutoConfigurationTest.java`

- [ ] **Step 11.1: Write the failing test**

Add to `XCanaryAutoConfigurationTest.java` (existing file):

```java
@Test
void kafkaListenerContainerFactoryHasCanaryRecordInterceptor() {
    runner
        .withBean(MeterRegistry.class, SimpleMeterRegistry::new)
        .withPropertyValues("canary.service-name=payment", "canary.presence-watcher.enabled=false")
        .run(ctx -> {
            ConcurrentKafkaListenerContainerFactory<?, ?> factory = ctx.getBean(
                    "kafkaListenerContainerFactory",
                    ConcurrentKafkaListenerContainerFactory.class);
            assertThat(factory.getContainerProperties()).isNotNull();
            // Reflection check that the interceptor field is non-null
            // (factory exposes it via getRecordInterceptor() in spring-kafka 4.x)
            assertThat(factory.getRecordInterceptor()).isInstanceOf(CanaryKafkaRecordInterceptor.class);
        });
}
```

(Update the existing class's imports as needed.)

Update the existing `xCanaryRequestFilter` test to expect a `CanaryMetrics` argument: any test that builds the filter with the no-arg ctor must change.

- [ ] **Step 11.2: Run the test, verify it fails**

Run: `./gradlew :platform:lib-java:test --tests "com.canary.platform.lib.XCanaryAutoConfigurationTest"`
Expected: `FAILED` — interceptor not registered.

- [ ] **Step 11.3: Implement**

Edit `platform/lib-java/src/main/java/com/canary/platform/lib/autoconfigure/XCanaryAutoConfiguration.java`:

(a) Update the `xCanaryRequestFilter` bean to take `CanaryMetrics`:

```java
@Bean
public XCanaryRequestFilter xCanaryRequestFilter(CanaryMetrics metrics) {
    return new XCanaryRequestFilter(metrics);
}
```

(b) Update the `kafkaListenerContainerFactory` bean to take and apply the interceptor:

```java
@Bean
@ConditionalOnMissingBean(name = "kafkaListenerContainerFactory")
public ConcurrentKafkaListenerContainerFactory<Object, Object> kafkaListenerContainerFactory(
        ConsumerFactory<Object, Object> consumerFactory,
        ConsumerAwareRebalanceListener rebalanceListener,
        CanaryKafkaRecordInterceptor<Object, Object> recordInterceptor) {
    ConcurrentKafkaListenerContainerFactory<Object, Object> factory =
            new ConcurrentKafkaListenerContainerFactory<>();
    factory.setConsumerFactory(consumerFactory);
    factory.getContainerProperties().setConsumerRebalanceListener(rebalanceListener);
    factory.setRecordInterceptor(recordInterceptor);
    return factory;
}
```

Add the necessary import for `CanaryMetrics` and `CanaryKafkaRecordInterceptor`.

- [ ] **Step 11.4: Run the test, verify it passes**

Run: `./gradlew :platform:lib-java:test --tests "com.canary.platform.lib.XCanaryAutoConfigurationTest"`
Expected: `BUILD SUCCESSFUL`.

- [ ] **Step 11.5: Run the full lib-java test suite**

Run: `./gradlew :platform:lib-java:test`
Expected: `BUILD SUCCESSFUL`. Catches any leftover Phase 1–3 test that constructed `XCanaryRequestFilter()` without args.

- [ ] **Step 11.6: Commit**

```bash
git add platform/lib-java/src/main/java/com/canary/platform/lib/autoconfigure/XCanaryAutoConfiguration.java \
        platform/lib-java/src/test/java/com/canary/platform/lib/XCanaryAutoConfigurationTest.java
git commit -m "feat(observability): wire CanaryMetrics into HTTP filter + interceptor onto Kafka factory"
```

---

## Task 12 — `TracingAutoConfiguration` and OTel exporter wiring

**Files:**
- Create: `platform/lib-java/src/main/java/com/canary/platform/lib/observability/TracingAutoConfiguration.java`

This auto-configuration is intentionally near-empty — it exists to:
1. Register a `MeterFilter` that prevents accidental `lane` tag inheritance on JVM/process meters.
2. Document via class JavaDoc which Spring Boot 4 auto-config beans take care of the actual exporter (we don't reimplement it).

The OTLP endpoint, sampling probability, and protocol all come from `application.yml` + Spring Boot 4 auto-config (`org.springframework.boot.actuate.autoconfigure.tracing.OtlpAutoConfiguration` + the OTel Spring starter). No bespoke beans needed for export.

- [ ] **Step 12.1: Write the failing test**

Create `platform/lib-java/src/test/java/com/canary/platform/lib/observability/TracingAutoConfigurationTest.java`:

```java
package com.canary.platform.lib.observability;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.config.MeterFilter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import static org.assertj.core.api.Assertions.assertThat;

class TracingAutoConfigurationTest {

    @Test
    void registersJvmLaneStripperMeterFilter() {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(TracingAutoConfiguration.class))
                .withUserConfiguration(TestSupport.class)
                .run(ctx -> {
                    assertThat(ctx).getBeans(MeterFilter.class).isNotEmpty();
                });
    }

    @Configuration
    static class TestSupport {
        @Bean MeterRegistry meterRegistry() { return new SimpleMeterRegistry(); }
    }
}
```

- [ ] **Step 12.2: Run the test, verify it fails**

Run: `./gradlew :platform:lib-java:test --tests "com.canary.platform.lib.observability.TracingAutoConfigurationTest"`
Expected: `FAILED` — `cannot find symbol TracingAutoConfiguration`.

- [ ] **Step 12.3: Implement**

Create `platform/lib-java/src/main/java/com/canary/platform/lib/observability/TracingAutoConfiguration.java`:

```java
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
```

- [ ] **Step 12.4: Run the test, verify it passes**

Run: `./gradlew :platform:lib-java:test --tests "com.canary.platform.lib.observability.TracingAutoConfigurationTest"`
Expected: `BUILD SUCCESSFUL`.

- [ ] **Step 12.5: Commit**

```bash
git add platform/lib-java/src/main/java/com/canary/platform/lib/observability/TracingAutoConfiguration.java \
        platform/lib-java/src/test/java/com/canary/platform/lib/observability/TracingAutoConfigurationTest.java
git commit -m "feat(observability): TracingAutoConfiguration strips inherited lane from non-canary meters"
```

---

## Task 13 — Register both auto-configurations in `AutoConfiguration.imports`

**Files:**
- Modify: `platform/lib-java/src/main/resources/META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports`

- [ ] **Step 13.1: Append the two new auto-config classes**

Edit the file. Final content:

```
com.canary.platform.lib.autoconfigure.XCanaryAutoConfiguration
com.canary.platform.lib.observability.CanaryMetricsAutoConfiguration
com.canary.platform.lib.observability.TracingAutoConfiguration
```

- [ ] **Step 13.2: Run the full lib-java suite**

Run: `./gradlew :platform:lib-java:test`
Expected: `BUILD SUCCESSFUL`. Sanity check that registration doesn't break anything.

- [ ] **Step 13.3: Commit**

```bash
git add platform/lib-java/src/main/resources/META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports
git commit -m "feat(observability): register CanaryMetricsAutoConfiguration + TracingAutoConfiguration"
```

---

## Task 14 — Update each service's `application.yml`

**Files:**
- Modify: `services/payment-service/src/main/resources/application.yml`
- Modify: `services/audit-service/src/main/resources/application.yml`
- Modify: `services/inventory-service/src/main/resources/application.yml`
- Modify: `services/order-service/src/main/resources/application.yml`
- Modify: `services/notification-service/src/main/resources/application.yml`

Apply the same changes to each `application.yml` (paths above). Below is the change for `payment-service`; replicate identically for the others (only `spring.application.name` differs, which is already correct).

- [ ] **Step 14.1: Add `prometheus` to actuator exposure**

In each yml, find the `management.endpoints.web.exposure.include` line and change:
```yaml
        include: health,info
```
to:
```yaml
        include: health,info,prometheus
```

- [ ] **Step 14.2: Add tracing config**

After the `management.endpoint.health.*` block (or before `app:`), insert:

```yaml
  tracing:
    sampling:
      probability: 1.0
  otlp:
    tracing:
      endpoint: ${OTLP_TRACING_ENDPOINT:http://jaeger-collector.istio-system:4317}
      transport: grpc
```

So the final `management:` block looks like:

```yaml
management:
  endpoints:
    web:
      exposure:
        include: health,info,prometheus
  endpoint:
    health:
      probes:
        enabled: true
      show-details: never
      group:
        readiness:
          include: readinessState
  tracing:
    sampling:
      probability: 1.0
  otlp:
    tracing:
      endpoint: ${OTLP_TRACING_ENDPOINT:http://jaeger-collector.istio-system:4317}
      transport: grpc
```

The default endpoint resolves at runtime: in cluster, the `Service` `jaeger-collector` in `istio-system` is the default Jaeger OTLP receiver; for local dev the env var `OTLP_TRACING_ENDPOINT` lets the operator override.

No `canary.service-name` yml entry needed — `CanaryMetricsAutoConfiguration` already resolves it via `@Value("${canary.service-name:${SERVICE_NAME:unknown}}")`. The `SERVICE_NAME` env var is set by the existing Helm chart for each pod.

- [ ] **Step 14.3: Build all services to confirm config parses**

Run: `./gradlew :services:payment-service:bootJar :services:audit-service:bootJar :services:inventory-service:bootJar :services:order-service:bootJar :services:notification-service:bootJar`
Expected: `BUILD SUCCESSFUL`. yml parse errors surface here.

- [ ] **Step 14.4: Commit**

```bash
git add services/payment-service/src/main/resources/application.yml \
        services/audit-service/src/main/resources/application.yml \
        services/inventory-service/src/main/resources/application.yml \
        services/order-service/src/main/resources/application.yml \
        services/notification-service/src/main/resources/application.yml
git commit -m "feat(services): expose /actuator/prometheus + configure OTLP tracing endpoint"
```

---

## Task 15 — Helm chart scrape annotations

**Files:**
- Modify: `deploy/helm/service-chart/templates/deployment.yaml`

- [ ] **Step 15.1: Add annotations to pod template metadata**

Edit `deploy/helm/service-chart/templates/deployment.yaml`. Change lines 14-16 from:

```yaml
    metadata:
      labels:
        {{- include "service-chart.labels" . | nindent 8 }}
```

to:

```yaml
    metadata:
      labels:
        {{- include "service-chart.labels" . | nindent 8 }}
      annotations:
        prometheus.io/scrape: "true"
        prometheus.io/port: "{{ .Values.ports.http }}"
        prometheus.io/path: "/actuator/prometheus"
```

- [ ] **Step 15.2: Lint the chart**

Run: `helm lint deploy/helm/service-chart`
Expected: `1 chart(s) linted, 0 chart(s) failed`.

- [ ] **Step 15.3: Render to verify annotations appear**

Run: `helm template test deploy/helm/service-chart -f deploy/helm/values/payment-service.yaml | grep -A2 prometheus.io`
Expected: three lines printed (`prometheus.io/scrape`, `prometheus.io/port`, `prometheus.io/path`).

- [ ] **Step 15.4: Commit**

```bash
git add deploy/helm/service-chart/templates/deployment.yaml
git commit -m "feat(helm): add Prometheus scrape annotations to pod template"
```

---

## Task 16 — End-to-end verification on a live cluster

**Files:** (no source changes — runtime verification only)

This task validates that the foundation works on a real cluster before declaring 5.a complete.

- [ ] **Step 16.1: Bring up the cluster (per existing project convention)**

Run: `make all`
Expected: cluster comes up; all services Ready. Per `feedback_e2e_inpod_probes.md`, do NOT use `kubectl exec ... curl` against Java pods; use `kubectl wait --for=condition=ready` and `kubectl port-forward` for any in-pod verification.

- [ ] **Step 16.2: Verify Prometheus scrapes the new metrics endpoint**

Port-forward Prometheus:
```bash
kubectl port-forward -n istio-system svc/prometheus 9090:9090 &
sleep 3
curl -s 'http://localhost:9090/api/v1/targets' | grep -o 'payment-service[^"]*' | head -5
```
Expected: at least one target line containing `payment-service` and `health: "up"` in the surrounding JSON. If empty, check the pod has the scrape annotation: `kubectl -n services get pod -l app=payment-service -o jsonpath='{.items[0].metadata.annotations}'`.

- [ ] **Step 16.3: Verify the canary metrics show up in Prometheus**

Drive a single HTTP request through one service (use the existing E2E client harness or curl against an Istio ingress port-forward). Then:
```bash
curl -s 'http://localhost:9090/api/v1/query?query=canary_request_total' | jq '.data.result | length'
```
Expected: a non-zero integer. If zero, verify the request actually hit the service via `kubectl logs -n services deploy/payment-service-stable | tail -20`.

- [ ] **Step 16.4: Verify Jaeger receives an HTTP span with `canary.lane` attribute**

Port-forward Jaeger:
```bash
kubectl port-forward -n istio-system svc/tracing 16686:80 &
sleep 3
curl -s 'http://localhost:16686/api/services' | jq '.data'
```
Expected: a list including the spring-application names (e.g., `payment-service`).

Search for canary-tagged spans:
```bash
curl -s 'http://localhost:16686/api/traces?service=payment-service&tag=canary.lane%3Astable&limit=5' | jq '.data | length'
```
Expected: at least 1.

- [ ] **Step 16.5: Verify the `canary_lane_active` gauge reports correctly**

Without a canary deployed:
```bash
curl -s 'http://localhost:9090/api/v1/query?query=canary_lane_active' | jq '.data.result[]' 
```
Expected: rows showing `lane=stable, value=1` and `lane=canary, value=0`.

Deploy a canary using `canary-ctl deploy payment` (or whichever existing command spins up a canary). Re-query:
Expected: `lane=canary, value=1`.

- [ ] **Step 16.6: Commit a brief verification log**

If anything in 16.2–16.5 needed troubleshooting fixes, capture them as additional commits before this step. The verification itself need not produce a commit — only fixes do.

---

## Self-Review

After implementing all tasks, run a fresh-eyes pass:

**Spec coverage check:**
- Spec §1 (four metrics) → Tasks 4 (CanaryMetrics), 5 (HTTP), 6 (Kafka), 7 (Restate), 8 (lane gauge). ✓
- Spec §1.5 T1 (HTTP tracer + canary.lane) → Tasks 12 (config) + 9 (filter) + 14 (yml). ✓
- Spec §1.5 T2 (Kafka propagation) → DEFERRED to 5.b per sub-phase plan. Out of 5.a scope.
- Spec §1.5 T3 (Restate propagation) → DEFERRED to 5.b. Out of 5.a scope.
- Spec §1 service-config (yml + scrape annotations) → Tasks 14 + 15. ✓
- Spec §1 wiring of shadow-mismatch counter at call sites → DEFERRED to 5.b (lib-java helper exists, services not yet calling it).

**Placeholder scan:**
- No "TBD"/"TODO"/"add appropriate" — every code block is complete.
- Every test has assertions and expected output is specified.
- Every command has expected output.

**Type consistency check:**
- `CanaryMetrics` constructor signature: `(MeterRegistry, String serviceName)` — used identically in Tasks 4, 5, 10, 11. ✓
- `XCanaryRequestFilter` constructor: `(CanaryMetrics)` — used in Tasks 5, 11. ✓
- `CanaryKafkaRecordInterceptor` constructor: `(CanaryMetrics)` — used in Tasks 6, 10, 11. ✓
- Tag names (`substrate, service, lane, outcome, target`) consistent across all metric assertions. ✓
- Lane string values (`"stable"`, `"canary"`) consistent. ✓

---

## Out of scope for 5.a (handed to 5.b)

- Wiring `CanaryRestateMeter.measure(...)` at each Restate handler call site in `services/{order,inventory,payment,notification}-service`. Helper exists; callers do not yet invoke it.
- T2 Kafka trace-context propagation (Spring Kafka `observationEnabled=true` wiring + producer/consumer span linkage).
- T3 Restate trace-context propagation (Restate StatefulSet `RESTATE_TRACING_ENDPOINT` + Java SDK W3C propagation verification).
- Wiring `CanaryMetrics.recordShadowMismatch(...)` at each Phase 2/3 shadow-comparison site.

---

## Plan complete

Plan complete and saved to `docs/superpowers/plans/2026-05-11-canary-release-phase-5-a-instrumentation-foundation.md`.

**Two execution options:**

**1. Subagent-Driven (recommended)** — fresh subagent per task, review between tasks, fast iteration. Matches the user's documented default plan-execution workflow (isolated worktree + subagent-driven + merge-back-to-main).

**2. Inline Execution** — execute tasks in this session using `executing-plans`, batch execution with checkpoints.

**Which approach?**
