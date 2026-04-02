package org.myorg.sut

import aws.sdk.kotlin.services.dynamodb.DynamoDbClient
import io.github.huherto.awsLambdaStream.EnvironmentConfig
import io.github.huherto.awsLambdaStream.FaultManager
import io.github.huherto.awsLambdaStream.PipelineAssembler
import io.github.huherto.awsLambdaStream.flavors.CorrelatePipeline
import io.github.huherto.awsLambdaStream.flavors.EvaluatePipeline
import io.github.huherto.awsLambdaStream.flavors.Pipeline
import io.github.huherto.awsLambdaStream.from.DynamodbAdapter
import io.github.huherto.awsLambdaStream.sinks.EventBridgePublishOptions

class TriggerContainer(
    val envConfig: EnvironmentConfig,
    val dynamoDbClient: DynamoDbClient,
    val eventBridgePublishOptions: EventBridgePublishOptions = EventBridgePublishOptions(envConfig),
    val faultManager: FaultManager = FaultManager(envConfig, eventBridgePublishOptions = eventBridgePublishOptions),
) {

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
            eventBridgePublishOptions = eventBridgePublishOptions,
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