package io.github.huherto.awsLambdaStream.sinks

import aws.sdk.kotlin.services.eventbridge.model.PutEventsRequest
import aws.sdk.kotlin.services.eventbridge.model.PutEventsRequestEntry
import io.github.huherto.awsLambdaStream.EnvironmentConfig
import io.github.huherto.awsLambdaStream.EventCodec
import io.github.huherto.awsLambdaStream.UnitOfWork
import io.github.huherto.awsLambdaStream.connectors.DefaultEventBridgeClientFactory
import io.github.huherto.awsLambdaStream.connectors.EventBridgeClientFactory
import io.github.huherto.awsLambdaStream.connectors.EventBridgeConnector
import io.github.huherto.awsLambdaStream.connectors.RetryConfig
import io.github.huherto.awsLambdaStream.extensions.*
import io.github.huherto.awsLambdaStream.serialization.SerializationStrategy
import io.github.huherto.awsLambdaStream.serialization.SerializationStrategyResolver
import io.github.huherto.awsLambdaStream.utils.adornStandardTags
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.*
import mu.KotlinLogging
import kotlin.time.Duration.Companion.milliseconds

interface EventPublisher {
    fun publish(flow: Flow<UnitOfWork>): Flow<UnitOfWork>
}

class EventPublisherInMemory : EventPublisher {
    private val uows = mutableListOf<UnitOfWork>()
    override fun publish(flow: Flow<UnitOfWork>): Flow<UnitOfWork> {
        return flow.onEach { uows.add(it) }
    }
    fun events() = uows.map{ it.event }.toList()
    fun faults() = uows.map{ it.fault }.toList()
    fun uows() = uows.toList()
}

class EventBridgePublisher(
    val envConfig: EnvironmentConfig,
    val busName: String = envConfig.busName()?: "undefined",
    val source: String = envConfig.busSource() ?: "custom",
    val maxPublishRequestSize: Int = envConfig.maxPublishRequestSize()
        ?: envConfig.maxRequestSize() ?: (256 * 1024),
    val batchSize: Int = envConfig.publishBatchSize() ?: envConfig.batchSize() ?: 10,
    val parallel: Int = (envConfig.publishParallel() ?: envConfig.parallel()?: 8),
    val endpointId: String? = envConfig.busEndPointId(),
    val handleErrors: Boolean = true,
    val clientFactory: EventBridgeClientFactory = DefaultEventBridgeClientFactory(envConfig = envConfig),
    val claimCheckStore: ClaimCheckStore? = null,
    private val eventCodec: EventCodec? = null,
    private val serializationStrategy: SerializationStrategy = SerializationStrategyResolver(envConfig).resolve(),
) : EventPublisher {

    @OptIn(ExperimentalCoroutinesApi::class)
    override fun publish(flow: Flow<UnitOfWork>): Flow<UnitOfWork> {
        return flow
            .map { adornStandardTags(envConfig, it) }
            .map { toPublishRequestEntry(it) }
            .chunked(batchSize)
            .map { batchedList -> UnitOfWork(pipeline = batchedList.first().pipeline, batch = batchedList) }
            .let { flow ->
                if (claimCheckStore != null) {
                    with(claimCheckStore) {
                        storeClaimCheck(flow)
                    }
                } else {
                    flow
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
            val logger = KotlinLogging.logger { }
            logger.info{"Busname: $busName, Source: $source"}
            val entry = PutEventsRequestEntry.Companion {
                eventBusName = busName
                source = this@EventBridgePublisher.source
                detailType = event.eventType()
                detail = eventCodec?.encode(event) ?: event.toString()
            }
            return uow.withPublishRequestEntry(entry)
        } else if (fault != null) {
            val logger = KotlinLogging.logger { }
            logger.info{"Busname: $busName, Source: $source"}
            val entry = PutEventsRequestEntry.Companion {
                eventBusName = busName
                source = this@EventBridgePublisher.source
                detailType = fault.type
                detail = serializationStrategy.serialize(fault)
            }
            return uow.withPublishRequestEntry(entry)
        }
        return uow
    }

    internal fun toPublishRequest(batchUow: UnitOfWork): UnitOfWork {

        val entries = batchUow.batch?.mapNotNull{ it.publishRequestEntry }
        if (entries.isNullOrEmpty()) return batchUow
        val putEventsRequest = PutEventsRequest.Companion {
            this.entries = entries
            this.endpointId = this@EventBridgePublisher.endpointId
        }
        return batchUow.withPublishRequest(putEventsRequest)
    }

    internal suspend fun putEvents(batchUow: UnitOfWork): UnitOfWork {

        if (batchUow.publishRequest != null) {

            val connector = EventBridgeConnector(
                pipelineId = batchUow.pipeline?.id ?: "undefined",
                envConfig = envConfig,
                retryConfig = RetryConfig(),
                timeout = envConfig.timeout()?.milliseconds ?: 1000.milliseconds,
                clientFactory = clientFactory
            )
            val publishResponse = connector.putEvents(batchUow.publishRequest!!)
            return batchUow.withPublishResponse(publishResponse)
        }

        return batchUow
    }

}
