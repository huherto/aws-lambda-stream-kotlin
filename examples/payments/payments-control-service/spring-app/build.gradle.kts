@file:Suppress("UnstableApiUsage")

import com.github.jengelman.gradle.plugins.shadow.transformers.PropertiesFileTransformer

plugins {
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.shadow)
    `jvm-test-suite`
}

group = "org.myorg"
version = "1.0"

repositories {
    mavenCentral()
}

// Exclude the production logger from all test configurations
configurations.matching { it.name.startsWith("test") }.configureEach {
    val loggerModule = libs.lambda.json.logger.get().module
    exclude(group = loggerModule.group, module = loggerModule.name)
}

dependencies {
    // The same domain, adapters and containers as the plain control service: only the wiring differs.
    implementation(project(":examples:payments:payments-control-service:app"))
    implementation(project(":spring"))

    implementation(platform(libs.spring.boot.bom))
    implementation(libs.spring.boot.autoconfigure)
    implementation(libs.kotlin.reflect) // Spring needs it to create Kotlin beans

    implementation(platform(libs.aws.sdk.bom))
    implementation(libs.aws.java.core)
    implementation(libs.aws.java.events)
    implementation(libs.aws.sdk.dynamodb)
    implementation(libs.aws.sdk.eventbridge)
    implementation(libs.kotlin.logging)
    implementation(libs.lambda.json.logger)

    testImplementation(testFixtures(project(":examples:payments:common-app")))
    testImplementation(testFixtures(project(":examples:payments:payments-control-service:app")))
    testImplementation(libs.spring.boot.test)
    testImplementation(libs.aws.lambda.java.tests)
    testImplementation(kotlin("test"))
    testImplementation(libs.kotest.assertions)
    testImplementation(libs.slf4j.simple)
}

tasks.shadowJar {
    archiveBaseName.set("payments-control-service-spring")
    archiveClassifier.set("")
    archiveVersion.set("")
    // Several jars ship Spring's and Java's registration files; the app needs every jar's entries, not the first
    // jar's. Shadow 9 drops duplicate files before its transformers run unless they are explicitly included.
    filesMatching(listOf("META-INF/services/**", "META-INF/spring.factories", "META-INF/spring/**")) {
        duplicatesStrategy = DuplicatesStrategy.INCLUDE
    }
    mergeServiceFiles()
    append("META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports")
    // Properties files: values of the same key are joined with commas, not overwritten.
    transform(PropertiesFileTransformer::class.java) {
        paths.addAll("META-INF/spring.factories", "META-INF/spring/aot.factories")
        mergeStrategy.set(PropertiesFileTransformer.MergeStrategy.Append)
        mergeSeparator.set(",")
    }
}

kotlin {
    jvmToolchain(21)
}

testing {
    suites {
        getByName<JvmTestSuite>("test") {
            useJUnitJupiter()
        }
    }
}
