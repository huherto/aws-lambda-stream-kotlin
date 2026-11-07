
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

dependencies {
    implementation(libs.aws.cdk)
    implementation(project(":examples:payments:common-infra"))
    testImplementation(kotlin("test"))
}

tasks.test {
    useJUnitPlatform()
}

application {
    mainClass.set("org.myorg.payments.AppKt")
}
