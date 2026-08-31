@file:Suppress("UnstableApiUsage")

plugins {
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.serialization)
    alias(libs.plugins.shadow)
}

group = "io.github.huherto.awsLambdaStream"
version = "1.0"

repositories {
    mavenCentral()
}

dependencies {

    implementation(platform(libs.aws.sdk.bom))

    implementation(libs.kotlinx.serialization.json)

    implementation(libs.aws.java.core)
    implementation(libs.aws.java.events)

    compileOnly(libs.aws.sdk.dynamodb)
    compileOnly(libs.aws.sdk.cloudwatch)
    compileOnly(libs.aws.sdk.eventbridge)
    compileOnly(libs.aws.sdk.kinesis)
    compileOnly(libs.aws.sdk.lambda)
    compileOnly(libs.aws.sdk.s3)
    compileOnly(libs.aws.sdk.sns)
    compileOnly(libs.aws.sdk.sqs)

    implementation(libs.kotlin.logging)
    compileOnly(libs.kotlin.reflect)

    implementation(libs.kotlinx.coroutines)
    implementation(libs.kotlinx.datetime)
    implementation(libs.uuid.generator)

    testImplementation(kotlin("test"))

    testImplementation(libs.aws.sdk.dynamodb)
    testImplementation(libs.aws.sdk.cloudwatch)
    testImplementation(libs.aws.sdk.eventbridge)
    testImplementation(libs.aws.sdk.kinesis)
    testImplementation(libs.aws.sdk.lambda)
    testImplementation(libs.aws.sdk.s3)
    testImplementation(libs.aws.sdk.sns)
    testImplementation(libs.aws.sdk.sqs)

    testImplementation(libs.aws.lambda.java.tests)
    testImplementation(libs.aws.lambda.java.serialization)
    testImplementation(libs.kotest.assertions)
    testImplementation(libs.kotest.property)
    testImplementation(libs.kotest.runner)
    testImplementation(libs.kotlin.reflect)
    testImplementation(libs.kotlinx.coroutines.test)
    testImplementation(libs.kotlinx.serialization.json)
    testImplementation(libs.mockk)
    testImplementation(libs.slf4j.simple)
}

tasks.shadowJar {
    archiveBaseName.set("core")
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