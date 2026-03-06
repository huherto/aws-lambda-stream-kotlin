package io.github.huherto.awsLambdaStream

import kotlinx.coroutines.flow.*
import java.util.*
import java.util.concurrent.ConcurrentLinkedQueue

class PipelineAssembler private constructor(
    private val pipelines: MutableList<Pipeline>
)
{

    private val logger = mu.KotlinLogging.logger {  }

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
            merged = merged
                .catchFailures()
                .onCompletion { cause ->
                    if (cause != null) {
                        logger.warn { "PipelineAssembler.onCompletion: cause=$cause" }
                    }
                    flushFaults()
                }
        }

        return merged
    }

    fun startPipeline(uow: UnitOfWork) : UnitOfWork { return uow}

    fun endPipeline(uow: UnitOfWork) : UnitOfWork { return uow}

    fun logError(exception: Throwable) {
        logger.error{
            "Exception in pipeline flow: ${exception.message}"
        }
    }

    val theFaults = ConcurrentLinkedQueue<FailureEvent>()

    fun Flow<UnitOfWork>.catchFailures(): Flow<UnitOfWork> = catch { exception ->
        logError(exception)
        if (exception is FailureException) {
            val functionName = EnvironmentConfig().awsLambdaFunctionName()?: "undefined"
            val failureEvent = FailureEvent().apply {
                id = UUID.randomUUID().toString()
                partitionKey = UUID.randomUUID().toString()
                type = "FAILURE_EVENT"
                timestamp = System.currentTimeMillis()
                tags = mutableMapOf(
                    "functionname" to functionName
                )
                failureException = exception
            }
            theFaults.add(failureEvent)
        }
        else {
            // Not sure what to do here.
            // throw exception
        }
    }

    val published =  ConcurrentLinkedQueue<FailureEvent>()

    fun publish(fault: FailureEvent) {
        published.add(fault)
    }

    suspend fun flushFaults() {

        val count = flow {
            while (true) {
                val fault = theFaults.poll()
                if (fault == null) {
                    break
                }
                emit(fault)
            }
        }
            .buffer()
            .onEach { fault -> publish(fault)}.count()

        logger.info { "PipelineAssembler.flushFaults: count=$count" }
    }
}