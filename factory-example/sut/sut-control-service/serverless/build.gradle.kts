
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
    implementation(libs.aws.java.core)
    implementation(libs.aws.java.events)
    implementation(libs.kotlin.stdlib)
    implementation(libs.aws.dynamodb.enhanced)
    implementation(libs.aws.dynamodb)
    implementation(libs.kotlinx.serialization.json)

    // Use LambdaLogger and avoid Log4j which is bigger.
    // implementation("org.apache.logging.log4j:log4j-to-slf4j:2.8.2")
    testImplementation(kotlin("test"))
    testImplementation(libs.aws.lambda.java.tests)
}

tasks.shadowJar {
    archiveBaseName.set("serverless")
    archiveClassifier.set("")
    archiveVersion.set("")
}

kotlin {
    jvmToolchain(21)
}

tasks.getByName<Test>("test") {
    useJUnitPlatform()
}