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

// Exclude the production logger from all test configurations
configurations.matching { it.name.startsWith("test") }.configureEach {
    val loggerModule = libs.lambda.json.logger.get().module
    exclude(group = loggerModule.group, module = loggerModule.name)
}

dependencies {
    implementation(platform(libs.aws.sdk.bom))

    implementation(project(":examples:payments:common-gateway"))
    implementation(libs.aws.sdk.s3)
    implementation(libs.kotlin.logging)
    implementation(libs.kotlinx.coroutines)
    implementation(libs.kotlinx.serialization.json)
    implementation(libs.lambda.json.logger)
    implementation(libs.slf4j.api)

    testImplementation(testFixtures(project(":examples:payments:common-app")))
    testImplementation(kotlin("test"))
    testImplementation(libs.aws.lambda.java.tests)
    testImplementation(libs.kotest.assertions)
    testImplementation(libs.kotlinx.coroutines.test)
    testImplementation(libs.slf4j.simple)
}

tasks.shadowJar {
    archiveBaseName.set("payments-bank-network-ingress-gateway")
    archiveClassifier.set("")
    archiveVersion.set("")
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
