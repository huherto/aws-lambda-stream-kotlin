@file:Suppress("UnstableApiUsage")

plugins {
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.serialization)
}

group = "org.myorg"
version = "1.0"

repositories {
    mavenCentral()
}

// Shared by the external-system gateways: the egress "act" step, the ingress "translate" step,
// the view of requests each ingress gateway joins responses to, and the plumbing of simulation mode.
dependencies {
    implementation(platform(libs.aws.sdk.bom))

    api(project(":aws"))
    api(project(":examples:payments:common-app"))

    // The framework's AWS module leaves SDK clients to each app; every gateway publishes and reads these.
    api(libs.aws.sdk.dynamodb)
    api(libs.aws.sdk.eventbridge)
    api(libs.aws.sdk.sqs)
    api(libs.aws.java.core)
    api(libs.aws.java.events)

    implementation(libs.kotlin.logging)
    implementation(libs.kotlinx.coroutines)
    implementation(libs.kotlinx.serialization.json)

    testImplementation(testFixtures(project(":examples:payments:common-app")))
    testImplementation(kotlin("test"))
    testImplementation(libs.kotest.assertions)
    testImplementation(libs.kotlinx.coroutines.test)
    testImplementation(libs.slf4j.simple)
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

tasks.withType<Test> {
    jvmArgs("-XX:+EnableDynamicAgentLoading")
}
