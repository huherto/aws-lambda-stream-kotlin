package io.github.huherto.awsLambdaStream

import mu.KotlinLogging

abstract class Pipeline {

    protected val logger = KotlinLogging.logger {  }

    fun printStartPipeline(uom: UnitOfWork) {
        logger.debug { "start type: ${uom.event?.type}, eid: ${uom.event?.id}" }
    }

    fun printEndPipeline(uom: UnitOfWork) {
        val redacted = trimAndRedacted(uom)
        logger.debug { "end type: ${uom.event?.type}, eid: ${uom.event?.id}, uow: $redacted" }
    }

    fun trimAndRedacted(uom: UnitOfWork) {
        uom
    }

}

