plugins {
    java
    id("io.spring.dependency-management") version "1.1.7" apply false
}

allprojects {
    group = "com.canary"
    version = "0.0.1-SNAPSHOT"
}

subprojects {
    apply(plugin = "java")
    apply(plugin = "io.spring.dependency-management")

    java {
        toolchain {
            languageVersion.set(JavaLanguageVersion.of(25))
        }
    }

    extensions.configure<io.spring.gradle.dependencymanagement.dsl.DependencyManagementExtension> {
        imports {
            mavenBom("org.springframework.boot:spring-boot-dependencies:4.0.4")
        }
    }

    tasks.withType<Test>().configureEach {
        useJUnitPlatform()
    }

    repositories {
        mavenCentral()
    }
}
