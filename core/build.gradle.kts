@file:Suppress("UnstableApiUsage")

plugins {
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.serialization)
    alias(libs.plugins.shadow)
}

group = "io.kopipes"
version = "1.0"

repositories {
    mavenCentral()
}

dependencies {
    implementation(libs.kotlinx.serialization.json)
    implementation(libs.kotlin.logging)
    compileOnly(libs.kotlin.reflect)
    implementation(libs.kotlinx.coroutines)
    implementation(libs.kotlinx.datetime)
    implementation(libs.uuid.generator)

    testImplementation(kotlin("test"))
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
