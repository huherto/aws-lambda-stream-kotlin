@file:Suppress("UnstableApiUsage")

plugins {
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.shadow)
}

group = "io.kopipes.spring"
version = "1.0"

repositories {
    mavenCentral()
}

dependencies {
    api(project(":aws"))

    // Spring Boot is compileOnly: the consuming application brings its own Boot version.
    // Apps that don't use Spring never see these classes.
    compileOnly(platform(libs.spring.boot.bom))
    compileOnly(libs.spring.boot.autoconfigure)

    // Same convention as :aws, the AWS SDK clients are supplied by the application.
    compileOnly(platform(libs.aws.sdk.bom))
    compileOnly(libs.aws.sdk.eventbridge)

    implementation(libs.kotlin.logging)

    testImplementation(platform(libs.spring.boot.bom))
    testImplementation(libs.spring.boot.autoconfigure)
    testImplementation(libs.spring.boot.test)
    testImplementation(libs.spring.test)
    testImplementation(libs.assertj.core)
    testImplementation(platform(libs.aws.sdk.bom))
    testImplementation(libs.aws.sdk.eventbridge)
    testImplementation(libs.aws.sdk.dynamodb)
    testImplementation(libs.aws.sdk.kinesis)
    testImplementation(libs.aws.sdk.sqs)
    testImplementation(kotlin("test"))
    testImplementation(libs.kotest.assertions)
    testImplementation(libs.kotlinx.coroutines)
    testImplementation(libs.slf4j.simple)
}

tasks.shadowJar {
    archiveBaseName.set("spring")
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
