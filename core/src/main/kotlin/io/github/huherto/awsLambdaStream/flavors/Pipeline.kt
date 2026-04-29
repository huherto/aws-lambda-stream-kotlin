package io.github.huherto.awsLambdaStream.flavors

import io.github.huherto.awsLambdaStream.FaultManager
import io.github.huherto.awsLambdaStream.UnitOfWork
import kotlinx.coroutines.flow.Flow
import mu.KotlinLogging

/**
 * Base type for all pipeline flavors.
 *
 * A pipeline connects an upstream [Flow] of [UnitOfWork] items to one or more processing stages and
 * returns a downstream flow with the processed units of work. Concrete implementations define the
 * actual filtering, mapping, persistence, publication, or evaluation behavior in [connect].
 *
 * The base class provides common structured logging helpers for pipeline start, step, and completion
 * messages. Logged unit-of-work values are passed through [trimAndRedacted] before being emitted so
 * subclasses can customize how sensitive or verbose fields are represented.
 *
 * @param id Unique identifier for this pipeline instance. The id is included in logs and propagated
 * by concrete pipeline implementations where needed.
 */
abstract class Pipeline(val id : String) {

    /**
     * Logger shared by pipeline implementations.
     */
    protected val logger = KotlinLogging.logger {  }

    /**
     * Logs the start of processing for a [UnitOfWork].
     *
     * The message includes the event type, event id, and pipeline id. Missing event values are logged
     * using safe fallback values.
     *
     * @param uow Unit of work entering the pipeline.
     */
    fun printStartPipeline(uow: UnitOfWork) {
        val eventId = uow.event?.id ?: "null"
        val eventType = uow.event?.eventType() ?: "unknown"
        val pipelineId = this.id
        logger.debug { "start type:${eventType}, eid:${eventId}, pipelineId:${pipelineId}" }
    }

    /**
     * Logs the completion of processing for a [UnitOfWork].
     *
     * The unit of work is first passed through [trimAndRedacted] to allow subclasses to remove,
     * shorten, or mask sensitive data before logging.
     *
     * @param uow Unit of work leaving the pipeline.
     */
    fun printEndPipeline(uow: UnitOfWork) {
        val redacted = trimAndRedacted(uow)
        val eventType = uow.event?.eventType() ?: "unknown"
        val pipelineId = this.id
        logger.debug { "end type:${eventType}, eid:${uow.event?.id}, pipelineId:${pipelineId}, uow: $redacted" }
    }

    /**
     * Logs an intermediate pipeline step.
     *
     * Use this helper from concrete implementations when a named processing stage should be visible
     * in logs. The unit of work is first passed through [trimAndRedacted].
     *
     * @param step Human-readable name of the pipeline step.
     * @param uow Unit of work being processed at the step.
     */
    fun printStepPipeline(step: String, uow: UnitOfWork) {
        val redacted = trimAndRedacted(uow)
        logger.info { "step: ${step}, eid:${uow.event?.id}, uow: $redacted" }
    }

    /**
     * Returns a log-safe representation of a [UnitOfWork].
     *
     * The default implementation returns a shallow copy unchanged. Override this method in subclasses
     * to trim large fields or redact sensitive values before unit-of-work details are written to logs.
     *
     * @param uow Unit of work to prepare for logging.
     * @return A copy or transformed representation suitable for log output.
     */
    fun trimAndRedacted(uow: UnitOfWork) : UnitOfWork {
        return uow.copy()
    }

    /**
     * Connects this pipeline to an upstream flow.
     *
     * Implementations should compose the processing stages for the pipeline and use [fm] around
     * stages that can fail so faulty units of work can be handled consistently.
     *
     * @param fm Fault manager used by pipeline stages to capture or filter failures.
     * @param fromFlow Upstream flow of units of work.
     * @return Downstream flow emitted by this pipeline.
     */
    abstract fun connect(fm: FaultManager, fromFlow: Flow<UnitOfWork>): Flow<UnitOfWork>

    /**
     * Returns a concise string representation containing the pipeline id.
     */
    override fun toString(): String {
        return "Pipeline(id=$id)"
    }

}
