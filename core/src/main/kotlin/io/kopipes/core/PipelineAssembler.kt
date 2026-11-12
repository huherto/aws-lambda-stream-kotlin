package io.kopipes.core

import io.kopipes.core.faults.FaultManager
import io.kopipes.core.flavors.Pipeline
import io.kopipes.core.metrics.updateMetrics
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.merge
import kotlinx.coroutines.flow.onCompletion

/** Assembles and runs one or more [Pipeline] instances against a shared upstream flow. */
class PipelineAssembler private constructor(builder : Builder) {

    private val logger = mu.KotlinLogging.logger { }

    private val pipelines = builder.pipelines

    private val faultManager = builder.faultManager ?: throw IllegalArgumentException("faultManager is required")

    private val pipelineMetrics = builder.pipelineMetrics

    fun getFaultManager(): FaultManager {
        return faultManager
    }

    /** Builder for [PipelineAssembler]. */
    class Builder {
        internal val pipelines = mutableListOf<Pipeline>()

        internal var faultManager: FaultManager? = null

        internal var pipelineMetrics: Boolean = false

        fun addPipeline(pipeline: Pipeline): Builder {
            pipelines.add(pipeline)
            return this
        }

        /** Takes the fault manager and metric switches from [context]. */
        fun context(context: KopipesContext) = apply {
            faultManager(context.faultManager)
            envConfig(context.envConfig)
        }

        /** Enables pipeline metrics when the `pipeline` metric is configured. */
        fun envConfig(envConfig: EnvironmentConfig) = apply { this.pipelineMetrics = envConfig.isMetricEnabled("pipeline") }

        fun pipelineMetrics(enabled: Boolean) = apply { this.pipelineMetrics = enabled }

        fun faultManager(faultManager: FaultManager) = apply { this.faultManager = faultManager }

        fun build(): PipelineAssembler {
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
        if (!pipelineMetrics) return uow
        return uow.updateMetrics { pm ->
            pm.startPipeline(uow.pipeline?.id ?: "default")
        }
    }

    fun endPipeline(uow: UnitOfWork): UnitOfWork {
        if (!pipelineMetrics) return uow
        return uow.updateMetrics { pm ->
            pm.endPipeline()
        }
    }


}