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
import io.github.huherto.awsLambdaStream.sinks.EventBridgePublishOptions
import io.github.huherto.awsLambdaStream.sinks.EventBridgePublisher
import io.github.huherto.awsLambdaStream.sinks.EventPublisher

class TriggerContainer(
    val envConfig: EnvironmentConfig,
    val dynamoDbClient: DynamoDbClient,
    val eventPublisher: EventPublisher,
    val faultManager: FaultManager = FaultManager(envConfig, eventPublisher),
) {

    companion object {
        fun build() : TriggerContainer {
            val envConfig = EnvironmentConfig()
            val dynamoDbClient = getDynamoDbClient(envConfig)
            val eventPublisherOptions = EventBridgePublishOptions(envConfig)
            val eventPublisher = EventBridgePublisher(eventPublisherOptions)
            return TriggerContainer(
                envConfig = envConfig,
                dynamoDbClient = dynamoDbClient,
                eventPublisher = eventPublisher,
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
            dynamoDbClient = dynamoDbClient,
            onEventClass = listOf(TrackedUnitEvent::class),
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