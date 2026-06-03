plugins {
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.serialization)
    alias(libs.plugins.shadow)
    application
}

group = "io.github.huherto.awsLambdaStream"
version = "1.0"

repositories {
    mavenCentral()
}

dependencies {
    implementation(platform(libs.aws.sdk.bom))

    implementation(libs.aws.sdk.lambda)
    implementation(libs.aws.sdk.s3)
    implementation(libs.kotlin.stdlib)
    implementation(libs.kotlinx.coroutines)
    implementation(libs.kotlinx.datetime)
    implementation(libs.kotlinx.serialization.json)

    testImplementation(kotlin("test"))
}

application {
    mainClass.set("org.myorg.tools.resubmit.MainKt")
}

tasks.shadowJar {
    archiveBaseName.set("resubmit-events")
    archiveClassifier.set("")
    archiveVersion.set("")
}

kotlin {
    jvmToolchain(21)
}

tasks.test {
    useJUnitPlatform()
}