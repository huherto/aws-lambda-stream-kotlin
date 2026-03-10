package io.github.huherto.awsLambdaStream

import kotlinx.coroutines.flow.Flow
import kotlinx.serialization.InternalSerializationApi
import kotlinx.serialization.serializerOrNull
import mu.KotlinLogging

abstract class Pipeline(val id : String) {

    protected val logger = KotlinLogging.logger {  }

    fun printStartPipeline(uom: UnitOfWork) {
        val eventType = eventType(uom.event)
        logger.debug { "start type:${eventType}, eid:${uom.event?.id ?: "null"}" }
    }

    @OptIn(InternalSerializationApi::class)
    private fun eventType(event: Event?): String {
        val type = event?.let { it::class.serializerOrNull()?.descriptor?.serialName } ?: "unknown"
        return type
    }

    fun printEndPipeline(uom: UnitOfWork) {
        val redacted = trimAndRedacted(uom)
        val eventType = eventType(uom.event)
        logger.debug { "end type:${eventType}, eid:${uom.event?.id}, uow: $redacted" }
    }

    // Not implemented yet. For now, redefine manually to redact specific fields.
    fun trimAndRedacted(uom: UnitOfWork) : UnitOfWork {
        return uom.copy()
    }

    abstract fun connect(fm: FaultManager, fromFlow: Flow<UnitOfWork>): Flow<UnitOfWork>
}

