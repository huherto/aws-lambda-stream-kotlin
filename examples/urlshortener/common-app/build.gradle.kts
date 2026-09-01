plugins {
    `java-library`
}

group = "org.myorg"
version = "1.0"

repositories {
    mavenCentral()
}

dependencies {
    implementation(project(":core"))
    implementation(platform(libs.aws.sdk.bom))
    implementation(libs.aws.sdk.dynamodb)
    implementation(libs.jackson.databind)
    implementation(libs.jackson.datatype.jsr310)
    implementation(libs.kotlin.stdlib)
    implementation(libs.kotlinx.coroutines)

    testImplementation(libs.kotest.assertions)
}

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(21))
    }
}

testing {
    suites {
        getByName<JvmTestSuite>("test") {
            useJUnitJupiter("5.11.4")
        }
    }
}
