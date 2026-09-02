package io.github.huherto.awsLambdaStream

import io.github.huherto.awsLambdaStream.faults.FaultManager
import io.github.huherto.awsLambdaStream.flavors.Pipeline
import io.github.huherto.awsLambdaStream.metrics.updateMetrics
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.merge
import kotlinx.coroutines.flow.onCompletion

/** Assembles and runs one or more [Pipeline] instances against a shared upstream flow. */
class PipelineAssembler private constructor(builder : Builder) {

    private val logger = mu.KotlinLogging.logger { }

    private val pipelines = builder.pipelines

    private val faultManager = builder.faultManager?: throw RuntimeException("faultManager is required")

    fun getFaultManager(): FaultManager {
        return faultManager
    }

    /** Builder for [PipelineAssembler]. */
    class Builder {
        internal val pipelines = mutableListOf<Pipeline>()

        internal var envConfig = GlobalRegistry.envConfig()

        internal var faultManager: FaultManager? = null

        fun addPipeline(pipeline: Pipeline): Builder {
            pipelines.add(pipeline)
            return this
        }

        fun envConfig(envConfig: EnvironmentConfig) = apply { this.envConfig = envConfig }

        fun faultManager(faultManager: FaultManager) = apply { this.faultManager = faultManager }

        fun build(): PipelineAssembler {
            if (faultManager == null) {
                faultManager = GlobalRegistry.faultManager()
            }
            return PipelineAssembler(this)
        }
    }

    /** Companion object for [PipelineAssembler]. */
    companion object {
        @JvmStatic
        fun builder(): Builder {
            return Builder()
        }
    }

    @JvmOverloads
    fun assemble(headFlow: Flow<UnitOfWork>, includeFaultHandler: Boolean = true): Flow<UnitOfWork> {

        val headFlow = headFlow

        val flows = mutableListOf<Flow<UnitOfWork>>()
        for (pipeline in pipelines) {
            var flow = headFlow
                .map { uow -> uow.copy(pipeline = pipeline) }
                .map { uow -> startPipeline(uow) }
            flow = pipeline.connect(faultManager, flow)
                .map { uow -> endPipeline(uow) }
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

    fun startPipeline(uow: UnitOfWork): UnitOfWork {
        if (!envConfig().isMetricEnabled("pipeline")) return uow
        return uow.updateMetrics { pm ->
            pm.startPipeline(uow.pipeline?.id ?: "default")
        }
    }

    fun endPipeline(uow: UnitOfWork): UnitOfWork {
        if (!envConfig().isMetricEnabled("pipeline")) return uow
        return uow.updateMetrics { pm ->
            pm.endPipeline()
        }
    }


}