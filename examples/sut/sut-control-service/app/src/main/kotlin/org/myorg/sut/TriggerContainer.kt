package org.myorg.sut

import io.github.huherto.awsLambdaStream.Event
import io.github.huherto.awsLambdaStream.GlobalRegistry
import io.github.huherto.awsLambdaStream.PipelineAssembler
import io.github.huherto.awsLambdaStream.UnitOfWork
import io.github.huherto.awsLambdaStream.connectors.DefaultDynamoDbClientFactory
import io.github.huherto.awsLambdaStream.filters.EventFilters
import io.github.huherto.awsLambdaStream.flavors.*
import io.github.huherto.awsLambdaStream.from.DynamodbAdapter
import io.github.huherto.awsLambdaStream.sinks.EventPublisher
import io.github.huherto.awsLambdaStream.sinks.EventsMicrostore
import io.github.huherto.awsLambdaStream.sinks.EventsMicrostoreImpl
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
        CorrelatePipeline(
            id = "corre1",
            correlationKeySupplier = { uow ->
                val event = uow.event as? TrackedUnitEvent
                event?.entity?.id ?: throw RuntimeException(
                    "Entity id is not set in TrackedUnitEvent"
                )
            },
            eventCodec = TrackedUnitEventCodec,
            eventsMicrostore = eventsMicrostore,
        )
    }

    private val evaluatePipeline1: Pipeline by lazy {
        EvaluatePipeline(
            id = "eval_vta",
            eventPublisher = eventPublisher,
            eventsMicrostore = eventsMicrostore,
            eventCodec = TrackedUnitEventCodec,
            eventFilter = EventFilters.name(TrackedUnitEvent.SHIPMENT_CREATED),
            higherOrderEmit = EmitOption.Basic(clazz = VerifyTargetAddressEvent::class.java),
        )
    }

    fun contactCustomer(
        uow: UnitOfWork,
        template: HigherOrderEventTemplate
    ) : List<Event> {
        val deliveryAttempts = uow.correlated?.filter { it is DeliveryAttemptedEvent }
        deliveryAttempts?.let {
            if (it.size == 1) return emptyList()
            val baseEvent = uow.event as? TrackedUnitEvent ?: return emptyList()
            val e1 = template.applyTemplate(ContactCustomerEvent(entity = baseEvent.entity))
            return listOf(e1)
        }
        return emptyList()
    }

    private val evaluatePipeline2: Pipeline by lazy {
        EvaluatePipeline(
            id = "eval2",
            eventPublisher = eventPublisher,
            eventsMicrostore = eventsMicrostore,
            eventCodec = TrackedUnitEventCodec,
            eventFilter = EventFilters.name(TrackedUnitEvent.DELIVERY_ATTEMPTED),
            higherOrderEmit = EmitOption.Custom(::contactCustomer),
            expression = { uow -> true },
        )
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