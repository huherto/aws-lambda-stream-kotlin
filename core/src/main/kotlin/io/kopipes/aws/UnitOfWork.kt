package io.kopipes.aws

import io.kopipes.aws.faults.FaultEvent
import io.kopipes.aws.flavors.Pipeline
import kotlin.reflect.KClass

/** A unit of work containing an event and its processing context. */
data class UnitOfWork @JvmOverloads constructor(
    val pipeline: Pipeline? = null,
    val record: Any? = null,
    val event: Event? = null,
    val fault: FaultEvent? = null,
    val key: String? = null,
    val sequenceNumber: String? = null,
    val shardId: String? = null,
    val timestamp: String? = null,
    val meta: Map<String, String?>? = null,
    val triggers: List<Event>? = null,
    val correlated: List<Event>? = null,
    val batch: List<UnitOfWork>? = null,
    val undecryptedEvent: Event? = null,
    val extensions: Map<KClass<*>, Any> = emptyMap(),
) {
    inline fun <reified T : Any> getExtension(): T? = extensions[T::class] as? T

    fun withExtension(extension: Any): UnitOfWork {
        return copy(extensions = extensions + (extension::class to extension))
    }
}



