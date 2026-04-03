package io.github.huherto.awsLambdaStream

import aws.smithy.kotlin.runtime.SdkBaseException
import io.github.huherto.awsLambdaStream.flavors.Pipeline
import io.github.huherto.awsLambdaStream.sinks.EventPublisher
import kotlinx.coroutines.flow.*
import java.util.*
import java.util.concurrent.ConcurrentLinkedQueue

class FaultManager constructor(
    private val envConfig: EnvironmentConfig,
    private val eventPublisher: EventPublisher,
    private val skipLogging: Boolean = false
) {

    private val logger = mu.KotlinLogging.logger { }
    
    private val theFaults = ConcurrentLinkedQueue<FailureEvent>()

    // We may no longer need this.
    class FaultManagerPipeline(id: String) : Pipeline(id) {
        override fun connect(
            fm: FaultManager,
            fromFlow: Flow<UnitOfWork>
        ): Flow<UnitOfWork> {
            // dummy implementation.
            return emptyList<UnitOfWork>().asFlow()
        }
    }

    private val faultManagerPipeline = FaultManagerPipeline("fault1")

    fun getFaults(): List<FailureEvent> {
        return theFaults.toList()
    }

    fun publisher() : EventPublisher {
        return eventPublisher
    }

    inline fun <R> Flow<UnitOfWork>.mapNotFaulty(
        crossinline block: (UnitOfWork) -> R?
    ): Flow<R> {
        return this
            .mapNotNull { item ->
                faulty(item, block)
            }
    }

    inline fun <R> faulty(uow: UnitOfWork, block: (UnitOfWork) -> R): R? {
        return try {
            block(uow)
        } catch (e: Throwable) {
            val failureException = FailureException(uow, e)
            redirectFailure(failureException)
            null
        }
    }

    private fun isRetriableException(exception: FailureException): Boolean {
        if (!envConfig.streamRetryEnabled()) return false
        if (exception.cause is SdkBaseException) {
            return (exception.cause as SdkBaseException).sdkErrorMetadata.isRetryable
        }
        return false
    }

    fun redirectFailure(ex: FailureException) {
        logError(ex)
        if (!isRetriableException(ex)) {
            val functionName = envConfig.awsLambdaFunctionName() ?: "undefined"
            val failureEvent = FailureEvent().apply {
                id = UUID.randomUUID().toString()
                partitionKey = UUID.randomUUID().toString()
                timestamp = System.currentTimeMillis()
                tags = mutableMapOf("functionname" to functionName)
                failureException = ex
            }
            theFaults.add(failureEvent)
        } else {
            throw ex
        }
    }

    fun logError(exception: Throwable) {
        if (!skipLogging) { // Use it to keep logs clean on unit tests.
            logger.error {
                "Exception in pipeline flow: ${exception.message}"
            }
        }
    }

    suspend fun flushFaults() : Int {
        val flow = flow {
            while (true) {
                val fault = theFaults.poll() ?: break
                val uow = UnitOfWork(pipeline = faultManagerPipeline, event = fault)
                emit(uow)
            }
        }
        val count = eventPublisher.publish(flow).count()
        if (!skipLogging) {
            logger.info { "flushFaults: count=$count" }
        }
        return count
    }
}