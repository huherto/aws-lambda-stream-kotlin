package org.myorg.sut

import io.kopipes.aws.AwsKopipesContext
import io.kopipes.aws.from.DynamodbAdapter
import io.kopipes.aws.sinks.DynamoDbEventsMicrostore
import io.kopipes.core.Event
import io.kopipes.core.PipelineAssembler
import io.kopipes.core.UnitOfWork
import io.kopipes.core.filters.EventFilters
import io.kopipes.core.flavors.CorrelatePipeline
import io.kopipes.core.flavors.EvaluatePipeline
import io.kopipes.core.sinks.EventPublisher
import io.kopipes.core.sinks.EventsMicrostore
import mu.KotlinLogging.logger

class TriggerContainer(
    val context: AwsKopipesContext,
    val eventsMicrostore: EventsMicrostore = DynamoDbEventsMicrostore(context),
    val eventPublisher: EventPublisher = context.eventPublisher,
    val correlatePipeline: CorrelatePipeline = CorrelatePipeline.builder()
        .id("corre1")
        .eventFilter(EventFilters.classes(TrackedUnitEvent::class))
        .eventsMicrostore(eventsMicrostore)
        .eventCodec(TrackedUnitEventCodec)
        .isCollectedEvent(DynamodbAdapter::forCollectedEvents)
        .normalizer(DynamodbAdapter.normalize(TrackedUnitEventCodec))
        .correlationKeySupplier { uow ->
            val event = uow.event as? TrackedUnitEvent
            event?.entity?.id ?: throw RuntimeException(
                "Entity id is not set in TrackedUnitEvent"
            )
        }
        .build(),
    val evaluatePipeline1: EvaluatePipeline = EvaluatePipeline.builder()
        .id("eval_vta")
        .eventPublisher(eventPublisher)
        .eventsMicrostore(eventsMicrostore)
        .eventCodec(TrackedUnitEventCodec)
        .eventFilter(EventFilters.name(TrackedUnitEvent.SHIPMENT_CREATED))
        .emit { uow ->
            val base = uow.event as ShipmentCreatedEvent
            listOf(VerifyTargetAddressEvent(entity = base.entity))
        }
        .build(),
    val evaluatePipeline2: EvaluatePipeline = EvaluatePipeline.builder()
        .id("eval2")
        .eventPublisher(eventPublisher)
        .eventsMicrostore(eventsMicrostore)
        .eventCodec(TrackedUnitEventCodec)
        .eventFilter(EventFilters.name(TrackedUnitEvent.DELIVERY_ATTEMPTED))
        .emit(Companion::contactCustomer)
        .expression { uow -> true }
        .build(),
    val assembler: PipelineAssembler = PipelineAssembler.builder()
        .context(context)
        .addPipeline(correlatePipeline)
        .addPipeline(evaluatePipeline1)
        .addPipeline(evaluatePipeline2)
        .build(),
    val dynamoDbAdapter: DynamodbAdapter = DynamodbAdapter(context),
) {
    companion object {
        private val log = logger {}

        fun contactCustomer(
            uow: UnitOfWork
        ): List<Event> {
            val deliveryAttempts = uow.correlated?.filterIsInstance<DeliveryAttemptedEvent>()
            deliveryAttempts?.let {
                if (it.size == 1) return emptyList()
                val baseEvent = uow.event as? TrackedUnitEvent ?: return emptyList()
                return listOf(ContactCustomerEvent(entity = baseEvent.entity))
            }
            return emptyList()
        }

        fun build(): TriggerContainer = TriggerContainer(AwsKopipesContext())
    }
}
