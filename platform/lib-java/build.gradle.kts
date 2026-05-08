plugins {
    `java-library`
}

dependencies {
    api(libs.spring.boot.autoconfigure)
    api(libs.spring.boot.starter.web)
    api(libs.spring.kafka)
    api(libs.restate.sdk.api)
    api(libs.restate.sdk.common)

    testImplementation(libs.spring.boot.starter.test)
    testImplementation(libs.junit.jupiter)
    testImplementation(libs.mockito.core)
    testImplementation(libs.assertj.core)
    testRuntimeOnly(libs.junit.platform.launcher)
}
