package io.kopipes.core.faults

import io.kopipes.core.FaultException
import io.kopipes.core.UnitOfWork
import io.kopipes.core.EnvironmentConfig
import io.kopipes.core.flavors.Pipeline
import io.kopipes.core.serialization.snapshots.DefaultUnitOfWorkSnapshotter
import io.kopipes.core.serialization.snapshots.UnitOfWorkSnapshotter
import io.kopipes.core.sinks.EventPublisher
import kotlinx.coroutines.flow.*
import mu.KotlinLogging
import java.util.concurrent.ConcurrentLinkedQueue

/** Handles failures that occur while processing pipeline flows. */
class FaultManager(
    private val eventPublisher: EventPublisher,
    private val skipErrorLogging: Boolean = false,
    private val isStreamRetryEnabled: Boolean = false,
    private val isItemLevelRetryEnabled: Boolean = false,
    private val awsLambdaFunctionName: String = "undefined",
    /** Turns units of work into diagnostic snapshots, for fault events and for pipeline debug logs. */
    val unitOfWorkSnapshotter: UnitOfWorkSnapshotter = DefaultUnitOfWorkSnapshotter(),
    private val faultEventFactory: FaultEventFactory = FaultEventFactory(
        awsLambdaFunctionName = awsLambdaFunctionName,
        unitOfWorkSnapshotter = unitOfWorkSnapshotter,
    ),
    private val isRetryable: (Throwable) -> Boolean = { false },
) {

    companion object {
        /** A fault manager whose retry switches and function name come from [envConfig]. */
        @JvmStatic
        @JvmOverloads
        fun fromConfig(
            envConfig: EnvironmentConfig,
            eventPublisher: EventPublisher,
            unitOfWorkSnapshotter: UnitOfWorkSnapshotter = DefaultUnitOfWorkSnapshotter(),
            isRetryable: (Throwable) -> Boolean = { false },
            skipErrorLogging: Boolean = false,
        ): FaultManager = FaultManager(
            eventPublisher = eventPublisher,
            skipErrorLogging = skipErrorLogging,
            isStreamRetryEnabled = envConfig.streamRetryEnabled(),
            isItemLevelRetryEnabled = envConfig.itemLevelRetryEnabled(),
            awsLambdaFunctionName = envConfig.awsLambdaFunctionName() ?: "undefined",
            unitOfWorkSnapshotter = unitOfWorkSnapshotter,
            isRetryable = isRetryable,
        )
    }

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

            // redirectFailure() will rethrow if the exception is retryable,
            // causing the pipeline to fail in the lambda handler.
            redirectFailure(faultException)
            null
        }
    }

    private fun isRetriableException(exception: FaultException): Boolean {
        val cause = exception.cause ?: exception
        return isRetryable(cause) || isRetryable(exception)
    }

    fun pollRetryableItems(): List<UnitOfWork> {
        val list = mutableListOf<UnitOfWork>()
        while (true) {
            val uow = retryableItems.poll() ?: break
            list.add(uow)
        }
        return list
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
