
plugins {
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.serialization)
}

group = "org.myorg"
version = "1.0"

repositories {
    mavenCentral()
}

dependencies {
    implementation(libs.aws.cdk)
    testImplementation(kotlin("test"))
}

tasks.test {
    useJUnitPlatform()
}

