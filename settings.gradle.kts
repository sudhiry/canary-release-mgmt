rootProject.name = "canary-release-mgmt"

dependencyResolutionManagement {
    repositories {
        mavenCentral()
    }
}

include("platform:lib-java")
include("platform:restate-defs-java")
