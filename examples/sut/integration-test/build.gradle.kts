@file:Suppress("UnstableApiUsage")

plugins {
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.serialization)
    alias(libs.plugins.shadow)
    `jvm-test-suite`
}

group = "org.myorg"
version = "1.0"

repositories {
    mavenCentral()
}

dependencies {
    // No dependencies needed here since this a integration testing module.
}

tasks.shadowJar {
    archiveBaseName.set("serverless")
    archiveClassifier.set("")
    archiveVersion.set("")
}

kotlin {
    jvmToolchain(21)
}

// Taken from https://docs.gradle.org/current/userguide/jvm_test_suite_plugin.html#sec:declare_an_additional_test_suite
testing {
    suites {
        val test by getting(JvmTestSuite::class) {
            useJUnitJupiter()
        }

        register<JvmTestSuite>("integrationTest") {
            useJUnitJupiter()
            dependencies {
                implementation(platform(libs.aws.sdk.bom))

                // Please keep these sorted alphabetically.
                implementation(libs.aws.java.events)
                implementation(libs.aws.sdk.dynamodb)
                implementation(libs.aws.sdk.eventbridge)
                implementation(libs.aws.sdk.kinesis)
                implementation(libs.aws.sdk.lambda)
                implementation(libs.aws.sdk.s3)
                implementation(libs.aws.sdk.sqs)
                implementation(libs.kotest.assertions)
                implementation(libs.kotlin.logging)
                implementation(libs.kotlinx.serialization.json)
                implementation(libs.slf4j.simple)
                implementation(libs.testcon.junit.jupiter)
                implementation(libs.testcon.localstack)
                implementation(libs.jackson.kotlin)

                implementation(project(":core"))
                implementation(project(":examples:sut:common-app"))
                implementation(project())

            }
            targets {
                all {
                    testTask.configure {
                        // Ensure integration tests run after unit tests
                        shouldRunAfter(test)
                    }
                }
            }
        }
    }
}

tasks.named("check") {
    dependsOn(testing.suites.named("integrationTest"))
}

tasks.named("integrationTest") {
}