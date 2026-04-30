package io.github.huherto.awsLambdaStream

import io.github.huherto.awsLambdaStream.flavors.Pipeline
import kotlinx.coroutines.flow.*

/**
 * Assembles and runs one or more [Pipeline] instances against a shared upstream flow.
 *
 * `PipelineAssembler` fans out the provided head [Flow] to each configured pipeline, attaches the
 * pipeline to each emitted [UnitOfWork], connects the pipeline stages, and then merges all resulting
 * downstream flows into a single output flow.
 *
 * The assembler also owns the shared [FaultManager] used by all connected pipelines. When fault
 * handling is enabled in [assemble], queued fault events are flushed after the merged flow completes.
 *
 * Create instances through [builder].
 *
 * @param builder Builder containing the pipelines and shared fault manager.
 */
class PipelineAssembler private constructor(builder : Builder) {

    private val logger = mu.KotlinLogging.logger { }

    private val pipelines = builder.pipelines

    private val faultManager = builder.faultManager?: throw RuntimeException("faultManager is required")

    /**
     * Returns the shared [FaultManager] used when connecting pipelines.
     */
    fun getFaultManager(): FaultManager {
        return faultManager
    }

    /**
     * Builder for [PipelineAssembler].
     *
     * Pipelines are connected in the order they are added, but they are executed as independent branches
     * from the same upstream flow and later merged by [PipelineAssembler.assemble].
     */
    class Builder {
        internal val pipelines = mutableListOf<Pipeline>()

        internal var envConfig = EnvironmentConfig()

        internal var faultManager: FaultManager? = null

        /**
         * Adds a pipeline branch to the assembler.
         *
         * @param pipeline Pipeline to connect to the upstream flow.
         * @return This builder.
         */
        fun addPipeline(pipeline: Pipeline): Builder {
            pipelines.add(pipeline)
            return this
        }

        /**
         * Sets the environment configuration associated with this assembler build.
         *
         * This value is currently stored on the builder for callers that configure assembler-related
         * dependencies consistently, but fault handling is supplied through [faultManager].
         *
         * @param envConfig Environment-backed configuration.
         * @return This builder.
         */
        fun envConfig(envConfig: EnvironmentConfig) = apply { this.envConfig = envConfig }

        /**
         * Sets the shared fault manager used by every configured pipeline.
         *
         * This value is required before [build] is called.
         *
         * @param faultManager Fault manager used for pipeline error handling and fault flushing.
         * @return This builder.
         */
        fun faultManager(faultManager: FaultManager) = apply { this.faultManager = faultManager }

        /**
         * Creates a [PipelineAssembler].
         *
         * @throws RuntimeException when no [FaultManager] has been configured.
         */
        fun build(): PipelineAssembler {
            return PipelineAssembler(this)
        }
    }

    companion object {
        /**
         * Creates a new [Builder].
         */
        fun builder(): Builder {
            return Builder()
        }
    }

    /**
     * Connects all configured pipelines to [headFlow] and merges their outputs.
     *
     * For each configured [Pipeline], the assembler:
     * 1. Copies each incoming [UnitOfWork] and attaches the current pipeline.
     * 2. Calls [startPipeline] before the pipeline is connected.
     * 3. Delegates processing to [Pipeline.connect] using the shared [FaultManager].
     * 4. Calls [endPipeline] for each item emitted by the pipeline.
     *
     * The resulting pipeline flows are merged into one downstream flow. When [includeFaultHandler] is
     * `true`, flow completion triggers [FaultManager.flushFaults]. If the merged flow completes with a
     * failure, the cause is logged before faults are flushed.
     *
     * @param headFlow Upstream flow of units of work to fan out to all configured pipelines.
     * @param includeFaultHandler Whether to flush queued fault events when the merged flow completes.
     * @return A merged flow containing units of work emitted by all configured pipelines.
     */
    fun assemble(headFlow: Flow<UnitOfWork>, includeFaultHandler: Boolean = true): Flow<UnitOfWork> {

        val headFlow = headFlow

        val flows = mutableListOf<Flow<UnitOfWork>>()
        for (pipeline in pipelines) {
            var flow = headFlow
                .map { uow -> uow.copy(pipeline = pipeline) }
                .onEach { uow -> startPipeline(uow) }
            flow = pipeline.connect(faultManager, flow)
                .onEach { uow -> endPipeline(uow) }
            flows.add(flow)
        }

        var merged = merge(*flows.toTypedArray())
        if (includeFaultHandler) {
            merged = merged
                .onCompletion { cause ->
                    if (cause != null) {
                        logger.warn { "PipelineAssembler.onCompletion: cause=$cause" }
                    }
                    faultManager.flushFaults()
                }
        }

        return merged
    }

    /**
     * Hook invoked immediately before a [UnitOfWork] enters its assigned pipeline.
     *
     * The default implementation returns [uow] unchanged. Override or extend this method if the
     * assembler needs to add metrics, tracing, or other start-of-pipeline behavior.
     *
     * @param uow Unit of work entering a pipeline branch.
     * @return The same or modified unit of work.
     */
    fun startPipeline(uow: UnitOfWork): UnitOfWork {
        return uow
    }

    /**
     * Hook invoked after a [UnitOfWork] has been emitted by its pipeline.
     *
     * The default implementation returns [uow] unchanged. Override or extend this method if the
     * assembler needs to add metrics, tracing, or other end-of-pipeline behavior.
     *
     * @param uow Unit of work emitted by a pipeline branch.
     * @return The same or modified unit of work.
     */
    fun endPipeline(uow: UnitOfWork): UnitOfWork {
        return uow
    }


}