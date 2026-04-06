package org.myorg.sut

import aws.sdk.kotlin.services.dynamodb.DynamoDbClient
import io.github.huherto.awsLambdaStream.EnvironmentConfig
import io.github.huherto.awsLambdaStream.FaultManager
import io.github.huherto.awsLambdaStream.PipelineAssembler
import io.github.huherto.awsLambdaStream.flavors.CorrelatePipeline
import io.github.huherto.awsLambdaStream.flavors.EvaluatePipeline
import io.github.huherto.awsLambdaStream.flavors.Pipeline
import io.github.huherto.awsLambdaStream.from.DynamodbAdapter
import io.github.huherto.awsLambdaStream.getDynamoDbClient
import io.github.huherto.awsLambdaStream.sinks.*

class TriggerContainer(
    val envConfig: EnvironmentConfig,
    val dynamoDbClient: DynamoDbClient,
    val eventPublisher: EventPublisher,
    val eventsMicrostore: EventsMicrostore,
    val faultManager: FaultManager,
) {

    companion object {
        fun build() : TriggerContainer {
            val envConfig = EnvironmentConfig()
            val dynamoDbClient = getDynamoDbClient(envConfig)
            val eventPublisherOptions = EventBridgePublishOptions(envConfig)
            val eventPublisher = EventBridgePublisher(eventPublisherOptions)
            val faultManager = FaultManager(envConfig, eventPublisher)
            val eventsMicrostore = EventsMicrostoreImpl(
                envConfig = envConfig,
                dynamoDbClient = dynamoDbClient,
                faultManager = faultManager,
            )
            return TriggerContainer(
                envConfig = envConfig,
                dynamoDbClient = dynamoDbClient,
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
            onEventClass = listOf(TrackedUnitEvent::class),
            eventsMicrostore = eventsMicrostore,
        )
    }

    private val evaluatePipeline: Pipeline by lazy {
        EvaluatePipeline(
            id = "eval1",
            envConfig = envConfig,
            dynamoDbClient = dynamoDbClient,
            onEventClass = listOf(TrackedUnitEvent::class),
            eventPublisher = eventPublisher,
        )
    }

    val assembler: PipelineAssembler by lazy {
        PipelineAssembler.Companion
            .builder()
            .faultManager(faultManager)
            .addPipeline(correlatePipeline)
            .addPipeline(evaluatePipeline)
            .build()
    }

    val dynamoDbAdapter = DynamodbAdapter(faultManager)

}