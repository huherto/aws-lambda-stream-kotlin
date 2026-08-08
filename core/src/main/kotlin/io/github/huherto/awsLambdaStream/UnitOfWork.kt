package io.github.huherto.awsLambdaStream

import io.github.huherto.awsLambdaStream.flavors.Pipeline
import kotlin.reflect.KClass

data class UnitOfWork(
    val pipeline: Pipeline? = null,
    val record: Any? = null,
    val event: Event? = null,
    val fault: io.github.huherto.awsLambdaStream.faults.FaultEvent? = null,
    val key: String? = null,
    val sequenceNumber: String? = null,
    val shardId: String? = null,
    val timestamp: String? = null,
    val meta: Map<String, String?>? = null,
    val triggers: List<Event>? = null,
    val correlated: List<Event>? = null,
    val batch: List<UnitOfWork>? = null,
    val extensions: Map<KClass<*>, Any> = emptyMap(),
) {
    /** Retrieves an extension of the specified type. */
    inline fun <reified T : Any> getExtension(): T? = extensions[T::class] as? T

    /** Returns a new UnitOfWork with the given extension attached. */
    fun withExtension(extension: Any): UnitOfWork {
        return copy(extensions = extensions + (extension::class to extension))
    }
}



