plugins {
    `java-library`
}

group = "org.myorg"
version = "1.0"

repositories {
    mavenCentral()
}

dependencies {
    implementation(project(":examples:urlshortener:common-infra"))
    implementation(libs.aws.cdk)
}

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(21))
    }
}
