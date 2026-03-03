package io.github.huherto.awsLambdaStream

import aws.smithy.kotlin.runtime.http.Headers
import kotlinx.coroutines.flow.Flow
import mu.KotlinLogging

abstract class Pipeline(val id : String) {

    protected val logger = KotlinLogging.logger {  }

    fun printStartPipeline(uom: UnitOfWork) {
        logger.debug { "start type: ${uom.event?.type}, eid: ${uom.event?.id}" }
    }

    fun printEndPipeline(uom: UnitOfWork) {
        val redacted = trimAndRedacted(uom)
        logger.debug { "end type: ${uom.event?.type}, eid: ${uom.event?.id}, uow: $redacted" }
    }

    // Not implemented yet. Redefine manually to redact specific fields.
    fun trimAndRedacted(uom: UnitOfWork) : UnitOfWork {
        return uom.copy()
    }

    abstract fun connect(fromFlow: Flow<UnitOfWork>) : Flow<UnitOfWork>

}

