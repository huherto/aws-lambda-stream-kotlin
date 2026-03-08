package io.github.huherto.awsLambdaStream

import aws.smithy.kotlin.runtime.SdkBaseException
import kotlinx.coroutines.flow.*
import java.util.*
import java.util.concurrent.ConcurrentLinkedQueue

class PipelineAssembler private constructor(val builder : Builder)
{

    private val logger = mu.KotlinLogging.logger {  }

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

        if (exception is FailureException && !isRetriableException(exception)) {
            val functionName = envConfig.awsLambdaFunctionName()?: "undefined"
            val failureEvent = FailureEvent().apply {
                id = UUID.randomUUID().toString()
                partitionKey = UUID.randomUUID().toString()
                timestamp = System.currentTimeMillis()
                tags = mutableMapOf(
                    "functionname" to functionName
                )
                failureException = exception
            }
            theFaults.add(failureEvent)
        }
        else {
            // It continues to be caught later.
            throw exception
        }
    }

    private fun isRetriableException(exception: FailureException) : Boolean{
        if (!envConfig.streamRetryEnabled()) return false
        if (exception.cause is SdkBaseException) {
           return (exception.cause as SdkBaseException).sdkErrorMetadata.isRetryable
        }
        return false
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
            .onEach { fault -> publish(fault)}
            .count()

        logger.info { "PipelineAssembler.flushFaults: count=$count" }
    }
}