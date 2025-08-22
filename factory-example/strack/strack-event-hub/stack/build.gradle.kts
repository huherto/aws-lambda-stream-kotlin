
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
    mainClass.set("org.myorg.example.AppKt")
}

tasks.register<Exec>("cdk_deploy") {
    // Define the command and its arguments

    commandLine("../../gradlew", "shadowJar")
    commandLine("cdk", "deploy")

    // Optionally, set the working directory for the command
    // workingDir = file("path/to/your/directory")

    // Optionally, capture the output of the command
    // standardOutput = ByteArrayOutputStream()
    // errorOutput = ByteArrayOutputStream()

    // Optionally, configure what happens on command failure
    // ignoreExitValue = true // Don't fail the build if the command exits with a non-zero code
}

