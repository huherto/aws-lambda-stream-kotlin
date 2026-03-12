package io.github.huherto.awsLambdaStream

import io.github.huherto.awsLambdaStream.flavors.Pipeline
import kotlinx.coroutines.flow.*

class PipelineAssembler private constructor(builder : Builder) {

    private val logger = mu.KotlinLogging.logger { }

    private val pipelines = builder.pipelines

    private val faultManager = builder.faultManager

    fun getFaultManager(): FaultManager {
        return faultManager
    }

    class Builder {
        internal val pipelines = mutableListOf<Pipeline>()

        internal var envConfig = EnvironmentConfig()

        internal var faultManager = FaultManager(envConfig)

        fun addPipeline(pipeline: Pipeline): Builder {
            pipelines.add(pipeline)
            return this
        }

        fun envConfig(envConfig: EnvironmentConfig) = apply { this.envConfig = envConfig }

        fun faultManager(faultManager: FaultManager) = apply { this.faultManager = faultManager }

        fun build(): PipelineAssembler {
            return PipelineAssembler(this)
        }
    }

    companion object {
        fun builder(): Builder {
            return Builder()
        }
    }

    fun assemble(headFlow: Flow<UnitOfWork>, includeFaultHandler: Boolean = true): Flow<UnitOfWork> {

        val headFlow = headFlow

        val flows = mutableListOf<Flow<UnitOfWork>>()
        for (pipeline in pipelines) {
            var flow = headFlow
                .filterNotNull()
                .mapNotNull { uow -> uow.copy(pipeline = pipeline) }
                .onEach { uow -> startPipeline(uow) }
            flow = pipeline.connect(faultManager, flow)
                .filterNotNull()
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

    fun startPipeline(uow: UnitOfWork): UnitOfWork {
        return uow
    }

    fun endPipeline(uow: UnitOfWork): UnitOfWork {
        return uow
    }


}