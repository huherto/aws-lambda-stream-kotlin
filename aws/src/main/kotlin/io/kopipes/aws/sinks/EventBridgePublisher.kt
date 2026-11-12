package io.kopipes.aws.sinks

import aws.sdk.kotlin.services.eventbridge.model.PutEventsRequest
import aws.sdk.kotlin.services.eventbridge.model.PutEventsRequestEntry
import io.kopipes.aws.AwsEnvironmentConfig
import io.kopipes.aws.connectors.EventBridgeClientFactory
import io.kopipes.aws.connectors.EventBridgeConnector
import io.kopipes.aws.extensions.*
import io.kopipes.core.UnitOfWork
import io.kopipes.core.metrics.withStepMetrics
import io.kopipes.core.resilience.RetryConfig
import io.kopipes.core.sinks.EventPublisher
import io.kopipes.core.utils.StandardTags
import io.kopipes.core.utils.adornStandardTags
import io.kopipes.core.utils.chunked
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.*
import kotlin.time.Duration.Companion.milliseconds

class EventBridgePublisher(
    val clientFactory: EventBridgeClientFactory,
    val busName: String = "undefined",
    val source: String = "custom",
    val maxPublishRequestSize: Int = 256 * 1024,
    val batchSize: Int = 10,
    val parallel: Int = 8,
    val endpointId: String? = null,
    val timeoutMillis: Long = 1000,
    val handleErrors: Boolean = true,
    val claimCheckStore: ClaimCheckStore? = null,
    val tags: StandardTags = StandardTags(),
    val stepMetrics: Boolean = false,
) : EventPublisher {

    /** Bus, sizes, tags and metrics from [envConfig]; [source] defaults to `BUS_SRC`. */
    @JvmOverloads
    constructor(
        envConfig: AwsEnvironmentConfig,
        clientFactory: EventBridgeClientFactory,
        claimCheckStore: ClaimCheckStore? = null,
        source: String = envConfig.busSource() ?: "custom",
    ) : this(
        clientFactory = clientFactory,
        busName = envConfig.busName() ?: "undefined",
        source = source,
        maxPublishRequestSize = envConfig.maxPublishRequestSize() ?: envConfig.maxRequestSize() ?: (256 * 1024),
        batchSize = envConfig.publishBatchSize() ?: envConfig.batchSize() ?: 10,
        parallel = envConfig.publishParallel() ?: envConfig.parallel() ?: 8,
        endpointId = envConfig.busEndPointId(),
        timeoutMillis = envConfig.timeout() ?: 1000,
        claimCheckStore = claimCheckStore,
        tags = StandardTags(envConfig),
        stepMetrics = envConfig.isMetricEnabled("step"),
    )

    @OptIn(ExperimentalCoroutinesApi::class)
    override fun publish(flow: Flow<UnitOfWork>): Flow<UnitOfWork> {
        return flow
            .map { adornStandardTags(it, tags) }
            .map { toPublishRequestEntry(it) }
            .chunked(batchSize)
            .map { batchedList -> UnitOfWork(pipeline = batchedList.first().pipeline, batch = batchedList) }
            .let { f ->
                if (claimCheckStore != null) {
                    with(claimCheckStore) {
                        storeClaimCheck(f)
                    }
                } else {
                    f
                }
            }
            .map { toPublishRequest(it) }
            .flatMapMerge(parallel) { batchUow ->
                flow {
                    emit(putEvents(batchUow))
                }
            }
            // for cleaner logging and testing downstream
            .flatMapConcat { batchUow ->
                batchUow.batch?.asFlow() ?: emptyFlow()
            }
    }

    internal fun toPublishRequestEntry(uow: UnitOfWork): UnitOfWork {
        val event = uow.event
        val fault = uow.fault

        if (event != null) {
            val entry = PutEventsRequestEntry.Companion {
                eventBusName = busName
                source = this@EventBridgePublisher.source
                detailType = event.eventType()
                detail = event.toString()
            }
            return uow.withPublishRequestEntry(entry)
        } else if (fault != null) {
            val entry = PutEventsRequestEntry.Companion {
                eventBusName = busName
                source = this@EventBridgePublisher.source
                detailType = fault.type
                detail = fault.toString()
            }
            return uow.withPublishRequestEntry(entry)
        }
        return uow
    }

    internal fun toPublishRequest(batchUow: UnitOfWork): UnitOfWork {
        val entries = batchUow.batch?.mapNotNull { it.publishRequestEntry }
        if (entries.isNullOrEmpty()) return batchUow
        val putEventsRequest = PutEventsRequest.Companion {
            this.entries = entries
            this.endpointId = this@EventBridgePublisher.endpointId
        }
        return batchUow.withPublishRequest(putEventsRequest)
    }

    internal suspend fun putEvents(batchUow: UnitOfWork): UnitOfWork {
        if (batchUow.publishRequest != null) {
            return batchUow.withStepMetrics("publish", stepMetrics) { uow ->
                val connector = EventBridgeConnector(
                    pipelineId = uow.pipeline?.id ?: "undefined",
                    retryConfig = RetryConfig(),
                    timeout = timeoutMillis.milliseconds,
                    clientFactory = clientFactory
                )
                val publishResponse = connector.putEvents(uow.publishRequest!!)
                uow.withPublishResponse(publishResponse)
            }
        }

        return batchUow
    }
}
