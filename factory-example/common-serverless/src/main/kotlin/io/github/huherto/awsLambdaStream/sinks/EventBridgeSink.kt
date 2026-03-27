package io.github.huherto.awsLambdaStream.sinks

import aws.sdk.kotlin.services.eventbridge.model.PutEventsRequest
import aws.sdk.kotlin.services.eventbridge.model.PutEventsRequestEntry
import io.github.huherto.awsLambdaStream.EnvironmentConfig
import io.github.huherto.awsLambdaStream.UnitOfWork
import io.github.huherto.awsLambdaStream.connectors.EventBridgeConnector
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.*


data class EventBridgePublishOptions(
    val envConfig: EnvironmentConfig,
    val busName: String = envConfig.busName()?: "undefined",
    val source: String = envConfig.busSource() ?: "custom",
    val maxPublishRequestSize: Int = envConfig.maxPublishRequestSize()
        ?: envConfig.maxRequestSize() ?: (256 * 1024),
    val batchSize: Int = envConfig.publishBatchSize()
        ?: envConfig.batchSize() ?: 10,
    val parallel: Int = envConfig.publishParallel()
        ?: envConfig.parallel()?: 8,
    val endpointId: String? = envConfig.busEndPointId(),
    val handleErrors: Boolean = true,
    val step: String = "publish"
)

class EventBridgeSink {

    companion object {


        private val logger = mu.KotlinLogging.logger {}

        /**
         * Represents the publishToEventBridge pipeline step.
         * Modeled as an extension function on Flow<UnitOfWork>.
         */
        @OptIn(ExperimentalCoroutinesApi::class)
        fun Flow<UnitOfWork>.publishToEventBridge(
            opt: EventBridgePublishOptions
        ): Flow<UnitOfWork> {
            return this
                .map { toPublishRequestEntry(it, opt) }
                .chunked(opt.batchSize)
                .map { batchedList -> UnitOfWork(pipeline = batchedList.first().pipeline, batch = batchedList) }
                .onEach { logger.info { "Batching ${it.batch?.size} events for pipeline ${it.pipeline?.id} to EventBridge"} }
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

                val connector = EventBridgeConnector(batchUow.pipeline!!.id)
                val publishResponse = connector.putEvents(batchUow.publishRequest)
                return batchUow.copy(publishResponse = publishResponse)
            }

            return batchUow
        }
    }
}