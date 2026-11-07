@file:Suppress("UnstableApiUsage")

plugins {
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.serialization)
    `jvm-test-suite`
}

group = "org.myorg"
version = "1.0"

repositories {
    mavenCentral()
}

kotlin {
    jvmToolchain(21)
}

// Integration tests against the stacks deployed on LocalStack. Run them with ../run_itests.sh,
// which finds the BFF's URL and passes it as PAYMENTS_BFF_URL.
testing {
    suites {
        val test = getByName<JvmTestSuite>("test") {
            useJUnitJupiter()
        }

        register<JvmTestSuite>("integrationTest") {
            useJUnitJupiter()
            dependencies {
                implementation(platform(libs.aws.sdk.bom))

                implementation(libs.aws.sdk.dynamodb)
                implementation(libs.aws.sdk.eventbridge)
                implementation(libs.aws.sdk.sqs)
                implementation(libs.kotest.assertions)
                implementation(libs.kotlin.logging)
                implementation(libs.kotlinx.coroutines)
                implementation(libs.kotlinx.serialization.json)
                implementation(libs.slf4j.simple)

                implementation(project(":examples:payments:common-app"))
            }
            targets {
                all {
                    testTask.configure {
                        shouldRunAfter(test)
                        // Payments run concurrently across test classes; the slowest (a late bank deposit) takes minutes.
                        systemProperty("junit.jupiter.execution.parallel.enabled", "true")
                        systemProperty("junit.jupiter.execution.parallel.mode.classes.default", "concurrent")
                        environment("PAYMENTS_BFF_URL", System.getenv("PAYMENTS_BFF_URL") ?: "")
                        outputs.upToDateWhen { false }
                        testLogging { events("passed", "failed", "skipped"); showStandardStreams = false }
                    }
                }
            }
        }
    }
}
