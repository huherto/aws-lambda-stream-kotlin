package io.kopipes.core.serialization.snapshots

import io.kopipes.core.EventReference
import kotlinx.serialization.Serializable

@Serializable
data class EventSnapshot(
    val id: String? = null,
    val type: String? = null,
    val timestamp: Long? = null,
    val partitionKey: String? = null,
    val tags: Map<String, String>? = null,
    val raw: String? = null,
    val eem: String? = null,
    val triggers: List<EventReference>? = null,
    val encoded: String? = null,
)
