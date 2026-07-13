package io.github.huherto.awsLambdaStream

import aws.smithy.kotlin.runtime.SdkBaseException
import com.fasterxml.uuid.Generators
import io.github.huherto.awsLambdaStream.flavors.Pipeline
import io.github.huherto.awsLambdaStream.sinks.EventPublisher
import kotlinx.coroutines.flow.*
import java.util.*
import java.util.concurrent.ConcurrentLinkedQueue

/**
 * Handles failures that occur while processing pipeline flows.
 *
 * `FaultManager` wraps pipeline operations, captures non-retriable failures as [FaultEvent]s,
 * and publishes those fault events through the configured [eventPublisher] when [flushFaults]
 * is called.
 *
 * When stream retry is enabled, retryable AWS SDK exceptions are rethrown so the upstream stream
 * processor can retry the batch. Non-retryable exceptions are converted into fault events and held
 * in an in-memory queue until flushed.
 *
 * @param envConfig Environment-backed configuration used to determine retry behavior and function name.
 * @param eventPublisher Publisher used to emit generated [FaultEvent]s.
 * @param skipErrorLogging When `true`, suppresses error logging. Useful for tests.
 * @param isStreamRetryEnabled Whether retryable stream-processing failures should be rethrown.
 * Defaults to [EnvironmentConfig.streamRetryEnabled].
 * @param awsLambdaFunctionName Lambda function name attached to generated fault-event tags.
 */
class FaultManager(
    val envConfig: EnvironmentConfig,
    private val eventPublisher: EventPublisher,
    private val skipErrorLogging: Boolean = false,
    private val isStreamRetryEnabled: Boolean = envConfig.streamRetryEnabled(),
    private val awsLambdaFunctionName: String = envConfig.awsLambdaFunctionName()?:"undefined"
) {

    private val logger = mu.KotlinLogging.logger { }

    private val theFaults = ConcurrentLinkedQueue<FaultEvent>()

    private val uuidV1Generator = Generators.timeBasedGenerator()

    /**
     * Internal placeholder pipeline used when publishing fault events.
     *
     * Fault events are emitted as [UnitOfWork] instances and therefore need an associated [Pipeline].
     * This pipeline is not intended to process regular application events.
     */
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

    /**
     * Returns a snapshot of currently queued fault events.
     */
    fun getFaults(): List<FaultEvent> {
        return theFaults.toList()
    }

    /**
     * Returns the event publisher used by this fault manager.
     */
    fun publisher() : EventPublisher {
        return eventPublisher
    }

    /**
     * Maps each [UnitOfWork] with [block], capturing any thrown exception as a fault.
     *
     * Items for which [block] throws are redirected through [redirectFailure] and omitted from the
     * returned flow.
     */
    inline fun <R> Flow<UnitOfWork>.mapNotFaulty(
        crossinline block: suspend (UnitOfWork) -> R?
    ): Flow<R> {
        return this
            .mapNotNull { item ->
                faulty(item, block)
            }
    }

    /**
     * Filters a flow while converting predicate failures into fault events.
     *
     * If [block] throws for an item, the exception is captured and that item is filtered out.
     */
    inline fun Flow<UnitOfWork>.filterNotFaulty(
        crossinline block: (UnitOfWork) -> Boolean
    ): Flow<UnitOfWork> {
        return this
            .filter { item ->
                faulty(item, block) == true
            }
    }

    /**
     * Executes [block] for [uow] and captures failures as fault events.
     *
     * @return The result of [block], or `null` when [block] throws.
     */
    suspend inline fun <R> faulty(uow: UnitOfWork, crossinline block: suspend (uow: UnitOfWork) -> R): R? {
        return try {
            block(uow)
        } catch (e: Throwable) {
            val faultException = FaultException(uow, e)
            redirectFailure(faultException)
            null
        }
    }

    /**
     * Returns whether [exception] should be rethrown to allow stream retry handling.
     *
     * Only AWS SDK exceptions marked retryable are considered retriable.
     */
    private fun isRetriableException(exception: FaultException): Boolean {
        if (exception.cause is SdkBaseException) {
            return (exception.cause as SdkBaseException).sdkErrorMetadata.isRetryable
        }
        return false
    }

    /**
     * Handles a pipeline failure.
     *
     * Retriable AWS SDK failures are rethrown so the stream processor can retry. Other failures are
     * converted into [FaultEvent] instances and queued for later publishing by [flushFaults].
     */
    fun redirectFailure(ex: FaultException) {
        logError(ex)

        if (isStreamRetryEnabled && isRetriableException(ex)) {
            // rethrow to allow stream retry handling.
            //
            // (i.e., kinesis will submit the batch again)
            throw ex
        }

        val functionName = awsLambdaFunctionName
        val pipelineId = ex.uow?.pipeline?.id ?: "undefined"
        val failureEvent = FaultEvent().apply {
            id = uuidV1Generator.generate().toString() // UUID v1. Time-based
            partitionKey = UUID.randomUUID().toString() // UUID v4. Uniform distribution.
            timestamp = System.currentTimeMillis()
            tags = mutableMapOf(
                "functionname" to functionName,
                "pipeline" to pipelineId
            )
            err = FaultEvent.Error(ex.cause?.javaClass?.simpleName, ex.message)
            uow = ex.uow
            faultException = ex
        }
        theFaults.add(failureEvent)
    }

    /**
     * Logs [exception] unless error logging has been disabled.
     */
    fun logError(exception: Throwable) {
        if (!skipErrorLogging) { // Use it to keep logs clean on unit tests.
            logger.error(exception) {
                "Exception in pipeline flow"
            }
        }
    }

    /**
     * Publishes all queued fault events and removes them from the queue.
     *
     * Each fault event is wrapped in a [UnitOfWork] associated with the internal fault-manager
     * pipeline before publishing.
     *
     * @return The number of fault events published.
     */
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