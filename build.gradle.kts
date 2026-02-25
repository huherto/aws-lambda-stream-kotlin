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