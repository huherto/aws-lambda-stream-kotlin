package io.kopipes.aws.sinks

import aws.sdk.kotlin.services.eventbridge.model.PutEventsRequest
import aws.sdk.kotlin.services.eventbridge.model.PutEventsRequestEntry
import io.kopipes.aws.awsEnvConfig
import io.kopipes.aws.connectors.DefaultEventBridgeClientFactory
import io.kopipes.aws.connectors.EventBridgeClientFactory
import io.kopipes.aws.connectors.EventBridgeConnector
import io.kopipes.aws.extensions.*
import io.kopipes.core.UnitOfWork
import io.kopipes.core.metrics.withStepMetrics
import io.kopipes.core.resilience.RetryConfig
import io.kopipes.core.sinks.EventPublisher
import io.kopipes.core.utils.adornStandardTags
import io.kopipes.core.utils.chunked
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.*
import kotlin.time.Duration.Companion.milliseconds

class EventBridgePublisher(
    val busName: String = awsEnvConfig().busName() ?: "undefined",
    val source: String = awsEnvConfig().busSource() ?: "custom",
    val maxPublishRequestSize: Int = awsEnvConfig().maxPublishRequestSize()
        ?: awsEnvConfig().maxRequestSize() ?: (256 * 1024),
    val batchSize: Int = awsEnvConfig().publishBatchSize() ?: awsEnvConfig().batchSize() ?: 10,
    val parallel: Int = (awsEnvConfig().publishParallel() ?: awsEnvConfig().parallel() ?: 8),
    val endpointId: String? = awsEnvConfig().busEndPointId(),
    val handleErrors: Boolean = true,
    val clientFactory: EventBridgeClientFactory = DefaultEventBridgeClientFactory(),
    val claimCheckStore: ClaimCheckStore? = null,
) : EventPublisher {

    @OptIn(ExperimentalCoroutinesApi::class)
    override fun publish(flow: Flow<UnitOfWork>): Flow<UnitOfWork> {
        return flow
            .map { adornStandardTags(it) }
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
            return batchUow.withStepMetrics("publish") { uow ->
                val connector = EventBridgeConnector(
                    pipelineId = uow.pipeline?.id ?: "undefined",
                    retryConfig = RetryConfig(),
                    timeout = awsEnvConfig().timeout()?.milliseconds ?: 1000.milliseconds,
                    clientFactory = clientFactory
                )
                val publishResponse = connector.putEvents(uow.publishRequest!!)
                uow.withPublishResponse(publishResponse)
            }
        }

        return batchUow
    }
}
