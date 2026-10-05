package org.myorg.sut

import io.kopipes.aws.connectors.DefaultDynamoDbClientFactory
import io.kopipes.aws.flavors.EvaluatePipeline
import io.kopipes.aws.from.DynamodbAdapter
import io.kopipes.aws.sinks.EventBridgePublisher
import io.kopipes.aws.sinks.EventsMicrostoreImpl
import io.kopipes.core.Event
import io.kopipes.core.PipelineAssembler
import io.kopipes.core.filters.EventFilters
import io.kopipes.core.flavors.CorrelatePipeline
import io.kopipes.core.sinks.EventPublisher
import io.kopipes.core.sinks.EventsMicrostore
import mu.KotlinLogging.logger

class TriggerContainer(
    val eventsMicrostore: EventsMicrostore,
    val eventPublisher: EventPublisher,
    val correlatePipeline: CorrelatePipeline = CorrelatePipeline.builder()
        .id("correlate-pipeline")
        .eventFilter(EventFilters.classes(TrackedUnitEvent::class))
        .eventsMicrostore(eventsMicrostore)
        .eventCodec(TrackedUnitEventCodec)
        .correlationKeySupplier { uow ->
            (uow.event as? TrackedUnitEvent)?.entity?.id
        }
        .build(),
    val evaluatePipeline: EvaluatePipeline = EvaluatePipeline.builder()
        .id("evaluate-pipeline")
        .eventFilter(EventFilters.classes(TrackedUnitEvent::class))
        .eventPublisher(eventPublisher)
        .eventsMicrostore(eventsMicrostore)
        .eventCodec(TrackedUnitEventCodec)
        .emit { uow ->
            val event = uow.event as? TrackedUnitEvent
            if (event != null && uow.correlated?.isNotEmpty() == true) {
                listOf<Event>(
                    ShipmentCreatedEvent(
                        id = event.id,
                        timestamp = event.timestamp,
                        partitionKey = event.partitionKey,
                        entity = event.entity,
                    )
                )
            } else {
                emptyList()
            }
        }
        .build(),
    val assembler: PipelineAssembler = PipelineAssembler.builder()
        .addPipeline(correlatePipeline)
        .addPipeline(evaluatePipeline)
        .build(),
    val dynamoDbAdapter: DynamodbAdapter = DynamodbAdapter(),
) {
    companion object {
        private val log = logger {}

        fun build(): TriggerContainer {
            val eventsMicrostore = EventsMicrostoreImpl(
                DefaultDynamoDbClientFactory(),
            )
            val eventPublisher = EventBridgePublisher()
            return TriggerContainer(
                eventsMicrostore = eventsMicrostore,
                eventPublisher = eventPublisher,
            )
        }
    }
}
