
plugins {
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.serialization)
    application
}

group = "org.myorg"
version = "1.0"

repositories {
    mavenCentral()
}

// The operator's tools for the payments subsystem on LocalStack: list faults, release a core checking request after
// checking it, and resubmit faults. Run through ../scripts/operator.sh.
dependencies {
    implementation(platform(libs.aws.sdk.bom))
    implementation(project(":aws"))
    implementation(libs.aws.sdk.dynamodb)
    implementation(libs.aws.sdk.lambda)
    implementation(libs.aws.sdk.s3)
    implementation(libs.kotlinx.coroutines)
    implementation(libs.kotlinx.datetime)
    implementation(libs.kotlinx.serialization.json)
    implementation(libs.slf4j.simple)
}

kotlin {
    jvmToolchain(21)
}

application {
    mainClass.set("org.myorg.payments.OperatorKt")
}
