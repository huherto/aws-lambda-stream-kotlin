package io.github.huherto.awsLambdaStream.flavors

import io.github.huherto.awsLambdaStream.UnitOfWork
import io.github.huherto.awsLambdaStream.faults.FaultManager
import io.github.huherto.awsLambdaStream.serialization.UnitOfWorkSnapshotSerializer
import kotlinx.coroutines.flow.Flow
import kotlinx.serialization.json.Json
import mu.KotlinLogging

/** Base type for all pipeline flavors. */
abstract class Pipeline(val id : String) {

    protected val logger = KotlinLogging.logger {  }


    private val jsonForLogs = Json {
        explicitNulls = false
        encodeDefaults = false
    }

    fun printStartPipeline(uow: UnitOfWork) {
        val eventId = uow.event?.id ?: "null"
        val eventType = uow.event?.eventType() ?: "unknown"
        val pipelineId = this.id
        logger.debug { "start type:${eventType}, eid:${eventId}, pipelineId:${pipelineId}" }
    }

    fun printEndPipeline(uow: UnitOfWork) {
        val uowAsString = uowAsString(trimAndRedacted(uow))
        val eventType = uow.event?.eventType() ?: "unknown"
        val pipelineId = this.id
        logger.debug { "end type:${eventType}, eid:${uow.event?.id}, pipelineId:${pipelineId}, uow: $uowAsString" }
    }

    fun printStepPipeline(step: String, uow: UnitOfWork) {
        val uowAsString = uowAsString(trimAndRedacted(uow))
        logger.info { "step: ${step}, eid:${uow.event?.id}, uow: $uowAsString" }
    }

    fun trimAndRedacted(uow: UnitOfWork) : UnitOfWork {
        return uow.copy()
    }

    fun uowAsString(uow: UnitOfWork) : String {
        return jsonForLogs.encodeToString(UnitOfWorkSnapshotSerializer, uow)
    }

    abstract fun connect(fm: FaultManager, fromFlow: Flow<UnitOfWork>): Flow<UnitOfWork>

    override fun toString(): String {
        return "{\"id\":\"$id\"}"
    }

}
