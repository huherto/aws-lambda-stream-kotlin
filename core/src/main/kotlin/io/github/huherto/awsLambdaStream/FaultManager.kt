package io.github.huherto.awsLambdaStream

import aws.smithy.kotlin.runtime.SdkBaseException
import io.github.huherto.awsLambdaStream.flavors.Pipeline
import io.github.huherto.awsLambdaStream.sinks.EventPublisher
import kotlinx.coroutines.flow.*
import java.util.*
import java.util.concurrent.ConcurrentLinkedQueue

class FaultManager(
    val envConfig: EnvironmentConfig,
    private val eventPublisher: EventPublisher,
    private val skipErrorLogging: Boolean = false,
    private val isStreamRetryEnabled: Boolean = envConfig.streamRetryEnabled(),
    private val awsLambdaFunctionName: String = envConfig.awsLambdaFunctionName()?:"undefined"
) {

    private val logger = mu.KotlinLogging.logger { }
    
    private val theFaults = ConcurrentLinkedQueue<FaultEvent>()

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

    fun getFaults(): List<FaultEvent> {
        return theFaults.toList()
    }

    fun publisher() : EventPublisher {
        return eventPublisher
    }

    inline fun <R> Flow<UnitOfWork>.mapNotFaulty(
        crossinline block: suspend (UnitOfWork) -> R?
    ): Flow<R> {
        return this
            .mapNotNull { item ->
                faulty(item, block)
            }
    }

    inline fun Flow<UnitOfWork>.filterNotFaulty(
        crossinline block: (UnitOfWork) -> Boolean
    ): Flow<UnitOfWork> {
        return this
            .filter { item ->
                faulty(item, block) == true
            }
    }

    suspend inline fun <R> faulty(uow: UnitOfWork, crossinline block: suspend (uow: UnitOfWork) -> R): R? {
        return try {
            block(uow)
        } catch (e: Throwable) {
            val faultException = FaultException(uow, e)
            redirectFailure(faultException)
            null
        }
    }

    private fun isRetriableException(exception: FaultException): Boolean {
        if (!isStreamRetryEnabled) return false
        if (exception.cause is SdkBaseException) {
            return (exception.cause as SdkBaseException).sdkErrorMetadata.isRetryable
        }
        return false
    }

    fun redirectFailure(ex: FaultException) {
        logError(ex)
        if (!isRetriableException(ex)) {
            val functionName = awsLambdaFunctionName
            val failureEvent = FaultEvent().apply {
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
        if (!skipErrorLogging) { // Use it to keep logs clean on unit tests.
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
        logger.debug { "flushFaults: count=$count" }
        return count
    }

}