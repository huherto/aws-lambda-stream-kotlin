plugins {
    `java-library`
}

group = "org.myorg"
version = "1.0"

repositories {
    mavenCentral()
}

dependencies {
    implementation(libs.aws.cdk)
}

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(21))
    }
}
