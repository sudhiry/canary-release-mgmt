rootProject.name = "canary-release-mgmt"

dependencyResolutionManagement {
    repositories {
        mavenCentral()
    }
}

include("platform:lib-java")
include("platform:restate-defs-java")
include("services:audit-service")
include("services:payment-service")
include("services:inventory-service")
