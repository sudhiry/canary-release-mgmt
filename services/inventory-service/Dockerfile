# syntax=docker/dockerfile:1
# Multi-stage Java build for inventory-service.
# Build context is the repo root: docker build -f services/inventory-service/Dockerfile -t canary-release-mgmt/inventory-service:dev .

FROM eclipse-temurin:25-jdk AS builder
WORKDIR /repo
COPY gradlew settings.gradle.kts build.gradle.kts /repo/
COPY gradle /repo/gradle
COPY platform/lib-java /repo/platform/lib-java
COPY platform/restate-defs-java /repo/platform/restate-defs-java
COPY services/audit-service /repo/services/audit-service
COPY services/payment-service /repo/services/payment-service
COPY services/inventory-service /repo/services/inventory-service
RUN ./gradlew :services:inventory-service:bootJar --no-daemon

FROM eclipse-temurin:25-jre
COPY --from=builder /repo/services/inventory-service/build/libs/*.jar /app.jar
ENTRYPOINT ["java", "-jar", "/app.jar"]
