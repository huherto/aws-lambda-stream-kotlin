package io.kopipes.core.serialization.snapshots

import kotlinx.serialization.Serializable

@Serializable
data class S3Snapshot(
    val bucket: String? = null,
    val key: String? = null,
)
