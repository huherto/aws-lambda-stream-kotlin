
plugins {
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.serialization)
    alias(libs.plugins.shadow)
    application
}

group = "org.myorg"
version = "1.0"

repositories {
    mavenCentral()
}

dependencies {
    implementation(libs.aws.cdk)
    testImplementation(kotlin("test"))
    implementation(project(":factory-example:common-stack"))
}

tasks.test {
    useJUnitPlatform()
}

application {
    mainClass.set("org.myorg.sut.AppKt")
}

tasks.register<Exec>("cdklocal_deploy") {
    //dependsOn(":factory-example:sut:sut-event-hub:serverless:shadowJar")
    dependsOn("shadowJar")
    commandLine("cdklocal", "deploy", "sut-control-service-local")
}

