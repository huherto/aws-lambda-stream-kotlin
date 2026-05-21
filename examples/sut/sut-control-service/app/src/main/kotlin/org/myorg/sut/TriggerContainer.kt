package org.myorg.sut

import io.github.huherto.awsLambdaStream.*
import io.github.huherto.awsLambdaStream.connectors.DefaultDynamoDbClientFactory
import io.github.huherto.awsLambdaStream.filters.EventFilters
import io.github.huherto.awsLambdaStream.flavors.*
import io.github.huherto.awsLambdaStream.from.DynamodbAdapter
import io.github.huherto.awsLambdaStream.sinks.*
import mu.KotlinLogging.logger

class TriggerContainer(
    val envConfig: EnvironmentConfig,
    val eventPublisher: EventPublisher,
    val eventsMicrostore: EventsMicrostore,
    val faultManager: FaultManager,
) {

    companion object {

        private val logger = logger {}

        fun build() : TriggerContainer {
            val envConfig = EnvironmentConfig()
            val dynamoDbClientFactory = DefaultDynamoDbClientFactory(envConfig)
            val eventPublisherOptions = EventBridgePublishOptions(envConfig)
            val eventPublisher = EventBridgePublisher(eventPublisherOptions)
            val faultManager = FaultManager(envConfig, eventPublisher)
            val eventsMicrostore = EventsMicrostoreImpl(
                envConfig = envConfig,
                dynamoDbClientFactory = dynamoDbClientFactory,
                faultManager = faultManager,
            )
            return TriggerContainer(
                envConfig = envConfig,
                eventPublisher = eventPublisher,
                eventsMicrostore = eventsMicrostore,
                faultManager = faultManager,
            )
        }
    }

    private val correlatePipeline: Pipeline by lazy {
        CorrelatePipeline(
            id = "corre1",
            envConfig = envConfig,
            unmarshall = { eventAsString: String -> jsonDecode(eventAsString) },
            correlationKey = { uow ->
                val event = uow.event as? TrackedUnitEvent
                event?.entity?.id ?: "no-correlation-key"
            },
            eventFilter = EventFilters.classes(TrackedUnitEvent::class),
            eventsMicrostore = eventsMicrostore,
        )
    }

    private val evaluatePipeline1: Pipeline by lazy {
        EvaluatePipeline(
            id = "eval_vta",
            envConfig = envConfig,
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
            val e1 = ContactCustomerEvent().apply {
                entity = baseEvent.entity
            }
            template.applyTemplate(e1)
            return listOf(e1)
        }
        return emptyList()
    }

    private val evaluatePipeline2: Pipeline by lazy {
        EvaluatePipeline(
            id = "eval2",
            envConfig = envConfig,
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
            .faultManager(faultManager)
            .addPipeline(correlatePipeline)
            .addPipeline(evaluatePipeline1)
            .addPipeline(evaluatePipeline2)
            .build()
    }

    val dynamoDbAdapter = DynamodbAdapter(faultManager)

}