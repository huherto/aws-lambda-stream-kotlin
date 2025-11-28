@file:Suppress("UnstableApiUsage")
plugins {
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.serialization)
    alias(libs.plugins.shadow)
}

group = "org.myorg"
version = "1.0"

repositories {
    mavenCentral()
}

dependencies {

    implementation(platform(libs.aws.sdk.bom))

    implementation(libs.aws.sdk.dynamodb)
    implementation(libs.aws.sdk.dynamodb.enhanced)
    implementation(libs.aws.sdk.lambda)
    implementation(libs.aws.java.core)
    implementation(libs.aws.java.eventbridge)
    implementation(libs.aws.java.events)
    implementation(libs.aws.java.sdk.xray)
    implementation(libs.aws.xray.recorder.sdk.aws.sdk.v2)
    implementation(libs.kotlin.stdlib)
    implementation(libs.kotlinx.serialization.json)

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

testing {
    suites {
        val test by getting(JvmTestSuite::class) {
            useJUnitJupiter()
        }

        register<JvmTestSuite>("integrationTest") {
            useJUnitJupiter()
            dependencies {
                implementation(project())
                implementation(libs.aws.java.eventbridge)
            }
        }
    }
}

//tasks.getByName<Test>("test") {
//    useJUnitPlatform()
//}
tasks.named("check") {
    dependsOn(testing.suites.named("integrationTest"))
}