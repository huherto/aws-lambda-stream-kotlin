package io.kopipes.aws.faults

import io.kopipes.aws.FaultException
import io.kopipes.aws.UnitOfWork
import io.kopipes.aws.envConfig
import io.kopipes.aws.flavors.Pipeline
import io.kopipes.aws.sinks.EventPublisher
import kotlinx.coroutines.flow.*
import mu.KotlinLogging
import java.util.*
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.CopyOnWriteArrayList

/**
 * Strategy interface for classifying whether an exception is retryable.
 */
fun interface RetryExceptionClassifier {
    fun isRetryable(throwable: Throwable): Boolean

    companion object {
        private val classifiers = CopyOnWriteArrayList<RetryExceptionClassifier>()

        fun register(classifier: RetryExceptionClassifier) {
            classifiers.add(classifier)
        }

        fun isRetryable(throwable: Throwable): Boolean {
            var current: Throwable? = throwable
            var depth = 0
            val visited = Collections.newSetFromMap(IdentityHashMap<Throwable, Boolean>())
            while (current != null && depth < 20 && visited.add(current)) {
                depth++
                for (classifier in classifiers) {
                    if (classifier.isRetryable(current)) return true
                }
                // Check reflection for AWS SDK or other retryable exceptions without compile-time coupling
                try {
                    val sdkBaseClass = Class.forName("aws.smithy.kotlin.runtime.SdkBaseException")
                    if (sdkBaseClass.isInstance(current)) {
                        val metadataMethod = sdkBaseClass.getMethod("getSdkErrorMetadata")
                        val metadata = metadataMethod.invoke(current)
                        if (metadata != null) {
                            val isRetryableMethod = metadata.javaClass.getMethod("isRetryable")
                            if (isRetryableMethod.invoke(metadata) == true) {
                                return true
                            }
                        }
                    }
                } catch (_: Throwable) {
                }
                current = current.cause
            }
            return false
        }
    }
}

/** Handles failures that occur while processing pipeline flows. */
class FaultManager(
    private val eventPublisher: EventPublisher,
    private val skipErrorLogging: Boolean = false,
    private val isStreamRetryEnabled: Boolean = envConfig().streamRetryEnabled(),
    private val isItemLevelRetryEnabled: Boolean = envConfig().itemLevelRetryEnabled(),
    private val awsLambdaFunctionName: String = envConfig().awsLambdaFunctionName() ?: "undefined",
    private val faultEventFactory: FaultEventFactory = FaultEventFactory(awsLambdaFunctionName = awsLambdaFunctionName),
    private val isRetryablePredicate: (Throwable) -> Boolean = { RetryExceptionClassifier.isRetryable(it) }
) {

    private val logger = KotlinLogging.logger { }

    private val theFaults = ConcurrentLinkedQueue<FaultEvent>()

    private val retryableItems = ConcurrentLinkedQueue<UnitOfWork>()

    /** Internal placeholder pipeline used when publishing fault events. */
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

    fun publisher(): EventPublisher {
        return eventPublisher
    }

    fun pollRetryableItems(): List<UnitOfWork> {
        val items = mutableListOf<UnitOfWork>()
        while (true) {
            val uow = retryableItems.poll() ?: break
            items.add(uow)
        }
        return items
    }

    inline fun <R> Flow<UnitOfWork>.mapNotFaulty(
        crossinline block: suspend (UnitOfWork) -> R?
    ): Flow<R> {
        return this
            .mapNotNull { item ->
                faulty(item, block)
            }
    }

    inline fun <R> mapNotFaultyFrom(
        source: Flow<UnitOfWork>,
        crossinline block: suspend (UnitOfWork) -> R?
    ): Flow<R> {
        return source.mapNotFaulty(block)
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

            // redirectFailure() will rethrow if the exception is retryable.
            // causing the pipeline to fail in the lambda handler.
            redirectFailure(faultException)
            null
        }
    }

    private fun isRetriableException(exception: FaultException): Boolean {
        val cause = exception.cause ?: exception
        return isRetryablePredicate(cause)
    }

    fun redirectFailure(ex: FaultException) {
        logError(ex)

        if (isStreamRetryEnabled && isRetriableException(ex)) {
            if (isItemLevelRetryEnabled && ex.uow != null) {
                // Save it so we can report failures at the item level.
                retryableItems.add(ex.uow!!)
                return
            }
            // rethrow to allow stream retry handling.
            // (i.e., kinesis will submit the batch again)
            throw ex
        }

        val failureEvent = faultEventFactory.createFaultEvent(ex.uow, ex)
        theFaults.add(failureEvent)
    }

    fun logError(exception: Throwable) {
        if (!skipErrorLogging) { // Use it to keep logs clean on unit tests.
            logger.error(exception) {
                "Exception in pipeline flow"
            }
        }
    }

    suspend fun flushFaults(): Int {
        val flow = flow {
            while (true) {
                val fault = theFaults.poll() ?: break
                val uow = UnitOfWork(pipeline = faultManagerPipeline, fault = fault)
                emit(uow)
            }
        }
        val count = eventPublisher.publish(flow).count()
        logger.debug { "flushFaults: count=$count" }
        return count
    }
}