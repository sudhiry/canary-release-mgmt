plugins {
    `java-library`
}

dependencies {
    api(libs.spring.boot.autoconfigure)
    api(libs.spring.boot.starter.web)
    api(libs.spring.boot.starter.actuator)
    api(libs.spring.kafka)
    api(libs.restate.sdk.api)
    api(libs.restate.sdk.common)
    api(libs.micrometer.tracing.bridge.otel)
    api(libs.opentelemetry.exporter.otlp)
    api(libs.opentelemetry.spring.boot.starter)

    implementation("io.fabric8:kubernetes-client:7.4.0")

    testImplementation(libs.spring.boot.starter.test)
    testImplementation(libs.junit.jupiter)
    testImplementation(libs.mockito.core)
    testImplementation(libs.assertj.core)
    testRuntimeOnly(libs.junit.platform.launcher)
}
