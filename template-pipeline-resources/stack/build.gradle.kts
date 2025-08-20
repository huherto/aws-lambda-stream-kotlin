import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    kotlin("jvm") version "2.2.10"
    application
    kotlin("plugin.serialization") version "1.5.20"
    id("com.gradleup.shadow") version "9.0.2"
}

group = "org.myorg"
version = "1.0"

repositories {
    mavenCentral()
}

dependencies {
    val cdkVersion = "2.211.0"
    implementation("software.amazon.awscdk:aws-cdk-lib:${cdkVersion}")
    implementation("software.constructs:constructs:${cdkVersion}")
    testImplementation(kotlin("test"))
}



tasks.test {
    useJUnitPlatform()
}

tasks.withType<KotlinCompile> {
    //kotlinOptions.jvmTarget = "1.8"
}

application {
    mainClass.set("org.myorg.example.AppKt")
}

