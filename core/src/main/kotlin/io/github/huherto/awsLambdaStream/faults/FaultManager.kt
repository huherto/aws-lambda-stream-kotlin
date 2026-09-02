package io.github.huherto.awsLambdaStream.faults

import aws.smithy.kotlin.runtime.SdkBaseException
import com.amazonaws.services.lambda.runtime.events.StreamsEventResponse
import io.github.huherto.awsLambdaStream.FaultException
import io.github.huherto.awsLambdaStream.UnitOfWork
import io.github.huherto.awsLambdaStream.envConfig
import io.github.huherto.awsLambdaStream.flavors.Pipeline
import io.github.huherto.awsLambdaStream.sinks.EventPublisher
import kotlinx.coroutines.flow.*
import mu.KotlinLogging
import java.util.concurrent.ConcurrentLinkedQueue

/** Handles failures that occur while processing pipeline flows. */
class FaultManager(
    private val eventPublisher: EventPublisher,
    private val skipErrorLogging: Boolean = false,
    private val isStreamRetryEnabled: Boolean = envConfig().streamRetryEnabled(),
    private val isItemLevelRetryEnabled: Boolean = envConfig().itemLevelRetryEnabled(),
    private val awsLambdaFunctionName: String = envConfig().awsLambdaFunctionName()?:"undefined",
    private val faultEventFactory: FaultEventFactory = FaultEventFactory(awsLambdaFunctionName = awsLambdaFunctionName)
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

    inline fun <R> mapNotFaultyFrom(
        source: Flow<UnitOfWork>,
        crossinline block: suspend (UnitOfWork) -> R?)
    : Flow<R> {
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

            // redirecFailure() will rethrow if the exception is retryable.
            // causing the pipeline to fail in the lambda handler.
            redirectFailure(faultException)
            null
        }
    }

    private fun isRetriableException(exception: FaultException): Boolean {
        if (exception.cause is SdkBaseException) {
            return (exception.cause as SdkBaseException).sdkErrorMetadata.isRetryable
        }
        return false
    }

    fun kinesisRetryableFailures(): List<StreamsEventResponse.BatchItemFailure> {
        val retryableBatchFailures = mutableListOf<StreamsEventResponse.BatchItemFailure>()

        while (true) {
            val uow = retryableItems.poll() ?: break
            retryableBatchFailures.add(StreamsEventResponse.BatchItemFailure(uow.sequenceNumber))
        }

        return retryableBatchFailures
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

    suspend fun flushFaults() : Int {
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