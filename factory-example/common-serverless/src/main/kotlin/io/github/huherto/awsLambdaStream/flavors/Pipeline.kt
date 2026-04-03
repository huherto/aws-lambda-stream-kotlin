package io.github.huherto.awsLambdaStream.flavors
import io.github.huherto.awsLambdaStream.FaultManager
import io.github.huherto.awsLambdaStream.UnitOfWork
import kotlinx.coroutines.flow.Flow
import mu.KotlinLogging
import aws.sdk.kotlin.services.dynamodb.model.AttributeValue as SdkAV

abstract class Pipeline(val id : String) {

    protected val logger = KotlinLogging.logger {  }

    fun printStartPipeline(uow: UnitOfWork) {
        val eventId = uow.event?.id ?: "null"
        val eventType = uow.event?.eventType() ?: "unknown"
        val pipelineId = this.id
        logger.debug { "start type:${eventType}, eid:${eventId}, pipelineId:${pipelineId}" }
    }

    fun printEndPipeline(uow: UnitOfWork) {
        val redacted = trimAndRedacted(uow)
        val eventType = uow.event?.eventType() ?: "unknown"
        logger.debug { "end type:${eventType}, eid:${uow.event?.id}, uow: $redacted" }
    }

    fun printStepPipeline(step: String, uow: UnitOfWork) {
        val redacted = trimAndRedacted(uow)
        val eventType = uow.event?.eventType() ?: "unknown"
        logger.debug { "step type:${eventType}, eid:${uow.event?.id}, uow: $redacted" }
    }

    // Not implemented yet. For now, you could redefine manually to redact specific fields.
    fun trimAndRedacted(uow: UnitOfWork) : UnitOfWork {
        return uow.copy()
    }

    abstract fun connect(fm: FaultManager, fromFlow: Flow<UnitOfWork>): Flow<UnitOfWork>

    override fun toString(): String {
        return "Pipeline(id=$id)"
    }

    protected fun nullableS(s: String?): SdkAV {
        return s?.let { SdkAV.S(it) } ?: SdkAV.Null(true)
    }

    protected fun nullableN(s: String?): SdkAV {
        return s?.let { SdkAV.N(it) } ?: SdkAV.Null(true)
    }

    protected fun nullableB(s: Boolean?): SdkAV {
        return s?.let { SdkAV.Bool(it) } ?: SdkAV.Null(true)
    }

}

