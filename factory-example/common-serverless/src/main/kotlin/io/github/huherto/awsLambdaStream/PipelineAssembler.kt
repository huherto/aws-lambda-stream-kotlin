package io.github.huherto.awsLambdaStream

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.merge
import kotlinx.coroutines.flow.onEach

class PipelineAssembler private constructor(
    private val pipelines: MutableList<Pipeline>
) {

    class Builder {
        private val pipelines = mutableListOf<Pipeline>()

        fun addPipeline(pipeline: Pipeline): Builder {
            pipelines.add(pipeline)
            return this
        }

        fun build(): PipelineAssembler {
            return PipelineAssembler(pipelines)
        }
    }

    companion object {
        fun builder(): Builder {
            return Builder()
        }
    }

    fun assemble(headFlow: Flow<UnitOfWork>, includeFaultHandler: Boolean = true): Flow<UnitOfWork> {

        var headFlow = headFlow
        if (includeFaultHandler) {
            headFlow = headFlow.catchFailures()
        }

        val flows = mutableListOf<Flow<UnitOfWork>>()
        for (pipeline in pipelines) {
            var flow = headFlow
                .map { uow -> uow.copy(pipeline = pipeline) }
                .onEach { uow -> startPipeline(uow) }
            flow = pipeline.connect(flow)
                .onEach { uow -> endPipeline(uow) }
            flows.add(flow)
        }

        var merged = merge(*flows.toTypedArray())
        if (includeFaultHandler) {
            merged = merged.catchFailures()
        }

        return merged
    }

    fun startPipeline(uow: UnitOfWork) : UnitOfWork { return uow}

    fun endPipeline(uow: UnitOfWork) : UnitOfWork { return uow}

}