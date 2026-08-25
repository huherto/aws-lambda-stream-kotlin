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

allprojects {
    tasks.register("dependencySizeReport") {
        group = "help"
        description = "Reports the sizes of runtime dependencies."
        doLast {
            val configuration = configurations.findByName("runtimeClasspath")
            if (configuration != null && configuration.isCanBeResolved) {
                val artifacts = configuration.resolvedConfiguration.resolvedArtifacts
                if (artifacts.isNotEmpty()) {
                    logger.lifecycle("\n============================================================")
                    logger.lifecycle("Dependency sizes for project ${project.path}")
                    val totalSize = artifacts
                        .sortedByDescending { it.file.length() }
                        .map { artifact ->
                            val size = artifact.file.length()
                            logger.lifecycle("${"%,d".format(size / 1024).padStart(10)} KB  ${artifact.moduleVersion.id.group}:${artifact.name}:${artifact.moduleVersion.id.version}")
                            size
                        }.sum()
                    logger.lifecycle("------------------------------------------------------------")
                    logger.lifecycle("${"%,d".format(totalSize / 1024).padStart(10)} KB  TOTAL (uncompressed)")
                    logger.lifecycle("============================================================\n")
                }
            }
        }
    }
}