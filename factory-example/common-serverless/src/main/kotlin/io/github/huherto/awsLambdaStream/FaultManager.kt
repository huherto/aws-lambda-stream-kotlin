package io.github.huherto.awsLambdaStream

import aws.smithy.kotlin.runtime.SdkBaseException
import kotlinx.coroutines.flow.buffer
import kotlinx.coroutines.flow.count
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.onEach
import java.util.*
import java.util.concurrent.ConcurrentLinkedQueue

class FaultManager constructor(
    private val envConfig: EnvironmentConfig = EnvironmentConfig()
) {

    private val logger = mu.KotlinLogging.logger { }
    
    private val theFaults = ConcurrentLinkedQueue<FailureEvent>()
    private val published = ConcurrentLinkedQueue<FailureEvent>()

    fun getPublished(): List<FailureEvent> {
        return published.toList()
    }
    
    fun getFaults(): List<FailureEvent> {
        return theFaults.toList()
    }

    inline fun <T, R> T.faulty(uom: UnitOfWork, block: T.() -> R): R? {
        return try {
            block()
        } catch (e: Throwable) {
            val failureException = FailureException(uom, e)
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

    fun publish(fault: FailureEvent) {
        logger.info { "FaultManager.publish: fault=$fault" }
        published.add(fault)
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
        logger.error {
            "Exception in pipeline flow: ${exception.message}"
        }
    }

    suspend fun flushFaults() {
        val count = flow {
            while (true) {
                val fault = theFaults.poll() ?: break
                emit(fault)
            }
        }
            .buffer()
            .onEach { fault -> publish(fault) }
            .count()

        logger.info { "flushFaults: count=$count" }
    }
}