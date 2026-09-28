package org.myorg.sut

import io.kopipes.aws.Event
import io.kopipes.aws.GlobalRegistry
import io.kopipes.aws.PipelineAssembler
import io.kopipes.aws.UnitOfWork
import io.kopipes.aws.connectors.DefaultDynamoDbClientFactory
import io.kopipes.aws.filters.EventFilters
import io.kopipes.aws.flavors.CorrelatePipeline
import io.kopipes.aws.flavors.EvaluatePipeline
import io.kopipes.aws.flavors.Pipeline
import io.kopipes.aws.from.DynamodbAdapter
import io.kopipes.aws.sinks.EventPublisher
import io.kopipes.aws.sinks.EventsMicrostore
import io.kopipes.aws.sinks.EventsMicrostoreImpl
import mu.KotlinLogging.logger

class TriggerContainer(
    val eventPublisher: EventPublisher,
    val eventsMicrostore: EventsMicrostore,
) {

    companion object {

        private val logger = logger {}

        fun build() : TriggerContainer {
            val dynamoDbClientFactory = DefaultDynamoDbClientFactory()
            val eventsMicrostore = EventsMicrostoreImpl(
                dynamoDbClientFactory = dynamoDbClientFactory,
            )
            return TriggerContainer(
                eventPublisher = GlobalRegistry.eventPublisher(),
                eventsMicrostore = eventsMicrostore,
            )
        }
    }

    private val correlatePipeline: Pipeline by lazy {
        CorrelatePipeline.builder()
            .id("corre1")
            .correlationKeySupplier { uow ->
                val event = uow.event as? TrackedUnitEvent
                event?.entity?.id ?: throw RuntimeException(
                    "Entity id is not set in TrackedUnitEvent"
                )
            }
            .eventCodec(TrackedUnitEventCodec)
            .eventsMicrostore(eventsMicrostore)
            .build()
    }

    private val evaluatePipeline1: Pipeline by lazy {
        EvaluatePipeline.builder()
            .id("eval_vta")
            .eventPublisher(eventPublisher)
            .eventsMicrostore(eventsMicrostore)
            .eventCodec(TrackedUnitEventCodec)
            .eventFilter(EventFilters.name(TrackedUnitEvent.SHIPMENT_CREATED))
            .emit { uow ->
                val base = uow.event as ShipmentCreatedEvent
                listOf(VerifyTargetAddressEvent(entity = base.entity))
            }
            .build()
    }

    fun contactCustomer(
        uow: UnitOfWork
    ) : List<Event> {
        val deliveryAttempts = uow.correlated?.filterIsInstance<DeliveryAttemptedEvent>()
        deliveryAttempts?.let {
            if (it.size == 1) return emptyList()
            val baseEvent = uow.event as? TrackedUnitEvent ?: return emptyList()
            return listOf(ContactCustomerEvent(entity = baseEvent.entity))
        }
        return emptyList()
    }

    private val evaluatePipeline2: Pipeline by lazy {
        EvaluatePipeline.builder()
            .id("eval2")
            .eventPublisher(eventPublisher)
            .eventsMicrostore(eventsMicrostore)
            .eventCodec(TrackedUnitEventCodec)
            .eventFilter(EventFilters.name(TrackedUnitEvent.DELIVERY_ATTEMPTED))
            .emit(::contactCustomer)
            .expression { uow -> true }
            .build()
    }

    val assembler: PipelineAssembler by lazy {
        PipelineAssembler
            .builder()
            .addPipeline(correlatePipeline)
            .addPipeline(evaluatePipeline1)
            .addPipeline(evaluatePipeline2)
            .build()
    }

    val dynamoDbAdapter = DynamodbAdapter()

}