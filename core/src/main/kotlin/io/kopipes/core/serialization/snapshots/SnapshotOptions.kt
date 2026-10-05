package io.kopipes.core.serialization.snapshots

data class SnapshotOptions(
    val includeStackTrace: Boolean = true,
    val maxStackTraceFrames: Int = 50,
    val includeCauseChain: Boolean = true,
    val maxCauseDepth: Int = 5,
    val maxDiagnosticStringLength: Int = 10_000,
)
