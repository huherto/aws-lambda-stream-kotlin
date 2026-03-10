package io.github.huherto.awsLambdaStream

import aws.smithy.kotlin.runtime.SdkBaseException
import aws.smithy.kotlin.runtime.retries.Outcome
import flushFaults
import kotlinx.coroutines.flow.*
import java.util.*
import java.util.concurrent.ConcurrentLinkedQueue

class PipelineAssembler private constructor(builder : Builder) {

    private val logger = mu.KotlinLogging.logger { }

    private val pipelines = builder.pipelines

    private val envConfig = builder.envConfig

    class Builder {
        internal val pipelines = mutableListOf<Pipeline>()

        internal val envConfig = EnvironmentConfig()

        fun addPipeline(pipeline: Pipeline): Builder {
            pipelines.add(pipeline)
            return this
        }

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

        var headFlow = headFlow

        val flows = mutableListOf<Flow<UnitOfWork>>()
        for (pipeline in pipelines) {
            var flow = headFlow
                .filterNotNull()
                .mapNotNull { uow -> uow.copy(pipeline = pipeline) }
                .onEach { uow -> startPipeline(uow) }
            flow = pipeline.connect(flow)
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
                    flushFaults()
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

    fun logError(exception: Throwable) {
        logger.error {
            "Exception in pipeline flow: ${exception.message}"
        }
    }


}