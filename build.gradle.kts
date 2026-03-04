plugins {
    alias(libs.plugins.kotlin.jvm) apply(false)
    alias(libs.plugins.serialization) apply(false)
    alias(libs.plugins.shadow) apply(false)
    idea
}

idea {
    module {
        isDownloadSources = true
        isDownloadJavadoc = true
    }
}

tasks.withType<Test> {
    // Set the default log level for slf4j-simple to debug
    //systemProperty("org.slf4j.simpleLogger.defaultLogLevel", "debug")

    // Optional: Only enable it for your specific package to avoid too much noise
    // systemProperty("org.slf4j.simpleLogger.log.io.github.huherto.awsLambdaStream", "debug")
    jvmArgs("-XX:+EnableDynamicAgentLoading")
}