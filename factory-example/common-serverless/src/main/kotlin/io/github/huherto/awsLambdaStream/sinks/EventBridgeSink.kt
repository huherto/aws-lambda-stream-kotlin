package io.github.huherto.awsLambdaStream.sinks

import aws.sdk.kotlin.services.eventbridge.model.PutEventsRequest
import aws.sdk.kotlin.services.eventbridge.model.PutEventsRequestEntry
import io.github.huherto.awsLambdaStream.EnvironmentConfig
import io.github.huherto.awsLambdaStream.UnitOfWork
import io.github.huherto.awsLambdaStream.connectors.EventBridgeClientFactory
import io.github.huherto.awsLambdaStream.connectors.EventBridgeClientFactoryImpl
import io.github.huherto.awsLambdaStream.connectors.EventBridgeConnector
import io.github.huherto.awsLambdaStream.connectors.RetryConfig
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.*
import kotlin.time.Duration.Companion.milliseconds

//class EventBridgeConnectorFactory(private val envConfig: EnvironmentConfig, private val retryConfig: RetryConfig? = null) {
//
//    fun timeoutInMills(): Long {
//        return envConfig.busTimeout() ?: envConfig.timeout() ?: 1000L
//    }
//
//    fun create(pipelineId: String?) : EventBridgeConnector {
//        val timeout = timeoutInMills().milliseconds
//        return EventBridgeConnector(
//            pipelineId = pipelineId?: "undefined",
//            retryConfig = retryConfig ?: RetryConfig(),
//            timeout = timeout,
//            )
//    }
//
//}

data class EventBridgePublishOptions(
    val envConfig: EnvironmentConfig,
    val busName: String = envConfig.busName()?: "undefined",
    val source: String = envConfig.busSource() ?: "custom",
    val maxPublishRequestSize: Int = envConfig.maxPublishRequestSize()
        ?: envConfig.maxRequestSize() ?: (256 * 1024),
    val batchSize: Int = envConfig.publishBatchSize() ?: envConfig.batchSize() ?: 10,
    val parallel: Int = (envConfig.publishParallel() ?: envConfig.parallel()?: 8),
    val endpointId: String? = envConfig.busEndPointId(),
    val handleErrors: Boolean = true,
    val step: String = "publish",
    val clientFactory: EventBridgeClientFactory = EventBridgeClientFactoryImpl(envConfig = envConfig)
)

class EventBridgeSink {

    companion object {

        private val logger = mu.KotlinLogging.logger {}

        @OptIn(ExperimentalCoroutinesApi::class)
        fun Flow<UnitOfWork>.publishToEventBridge(
            opt: EventBridgePublishOptions
        ): Flow<UnitOfWork> {
            return this
                .map { toPublishRequestEntry(it, opt) }
                .chunked(opt.batchSize)
                .map { batchedList -> UnitOfWork(pipeline = batchedList.first().pipeline, batch = batchedList) }
                .map { toPublishRequest(it, opt) }
                .flatMapMerge(opt.parallel) { batchUow ->
                    flow {
                        emit(putEvents(batchUow, opt))
                    }
                }
                // for cleaner logging and testing downstream
                .flatMapConcat { batchUow ->
                    batchUow.batch?.asFlow() ?: emptyFlow()
                }
        }

        internal fun toPublishRequestEntry(uow: UnitOfWork, opt: EventBridgePublishOptions): UnitOfWork {
            val event = uow.event
            if (event != null) {
                 val entry = PutEventsRequestEntry.Companion {
                     eventBusName = opt.busName
                     source = opt.source
                     detailType = event.eventType()
                     detail = event.encoded()
                 }
                return uow.copy(publishRequestEntry = entry )
            }
            return uow
        }

        internal fun toPublishRequest(batchUow: UnitOfWork, opt: EventBridgePublishOptions): UnitOfWork {

            val entries = batchUow.batch?.mapNotNull{ it.publishRequestEntry }
            val putEventsRequest = PutEventsRequest.Companion {
                this.entries = entries
                this.endpointId = opt.endpointId
            }
            return batchUow.copy(publishRequest = putEventsRequest)
        }

        internal suspend fun putEvents(batchUow: UnitOfWork, opt: EventBridgePublishOptions): UnitOfWork {

            if (batchUow.publishRequest != null) {

                val connector = EventBridgeConnector(
                    pipelineId = batchUow.pipeline?.id ?: "undefined",
                    envConfig = opt.envConfig,
                    retryConfig = RetryConfig(),
                    timeout = opt.envConfig.timeout()?.milliseconds ?: 1000.milliseconds,
                    clientFactory = opt.clientFactory
                    )
                val publishResponse = connector.putEvents(batchUow.publishRequest)
                return batchUow.copy(publishResponse = publishResponse)
            }

            return batchUow
        }
    }
}