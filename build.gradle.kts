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

allprojects {
    plugins.withId("org.jetbrains.kotlin.jvm") {
        extensions.configure<org.jetbrains.kotlin.gradle.dsl.KotlinJvmProjectExtension> {
            jvmToolchain(21)
        }
    }
    tasks.withType<Test> {
        maxHeapSize = "2048m"
        jvmArgs("-XX:+EnableDynamicAgentLoading", "-Xmx2048m")
    }
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