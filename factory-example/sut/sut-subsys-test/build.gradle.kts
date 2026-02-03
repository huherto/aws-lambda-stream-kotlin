@file:Suppress("UnstableApiUsage")

import org.gradle.kotlin.dsl.invoke

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

    implementation(platform(libs.aws.sdk.bom))

    implementation(project(":factory-example:common-serverless"))

    implementation(libs.aws.java.core)
    implementation(libs.aws.java.events)
    implementation(libs.aws.sdk.dynamodb)
    implementation(libs.aws.sdk.lambda)
    implementation(libs.kotlin.stdlib)
    implementation(libs.kotlinx.serialization.json)
    implementation(libs.lambda.json.logger)

    // Use LambdaLogger and avoid Log4j which is bigger.
    // implementation("org.apache.logging.log4j:log4j-to-slf4j:2.8.2")

    testImplementation(kotlin("test"))
    testImplementation(libs.aws.lambda.java.tests)
    testImplementation(libs.mockk)
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
                implementation(libs.aws.sdk.kinesis)
                implementation(libs.aws.sdk.lambda)
                implementation(libs.aws.sdk.eventbridge)
                implementation(libs.testcon.localstack)
                implementation(libs.testcon.junit.jupiter)
                implementation(libs.slf4j.simple)
                implementation(platform(libs.aws.sdk.bom))
                implementation(project(":factory-example:common-serverless"))
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