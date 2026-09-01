plugins {
    `java-library`
    alias(libs.plugins.shadow)
}

group = "org.myorg"
version = "1.0"

repositories {
    mavenCentral()
}

dependencies {
    implementation(project(":core"))
    implementation(project(":examples:urlshortener:common-app"))

    implementation(platform(libs.aws.sdk.bom))
    implementation(libs.aws.sdk.dynamodb)
    implementation(libs.aws.sdk.eventbridge)
    implementation(libs.aws.sdk.kinesis)
    implementation(libs.aws.java.core)
    implementation(libs.aws.java.events)

    implementation(libs.kotlinx.coroutines)
    implementation(libs.kotlin.stdlib)

    implementation(libs.jackson.databind)
    implementation(libs.jackson.datatype.jsr310)

    implementation(libs.slf4j.api)

    testImplementation(libs.kotest.assertions)
}

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(21))
    }
}

tasks.shadowJar {
    archiveBaseName.set("serverless")
    archiveClassifier.set("")
    archiveVersion.set("")
}

testing {
    suites {
        getByName<JvmTestSuite>("test") {
            useJUnitJupiter("5.11.4")
        }
    }
}
