package io.github.huherto.awsLambdaStream.serialization.snapshots

import kotlinx.serialization.Serializable

@Serializable
data class ErrorSnapshot(
    val name: String? = null,
    val message: String? = null,
    val stackTrace: List<String>? = null,
)
