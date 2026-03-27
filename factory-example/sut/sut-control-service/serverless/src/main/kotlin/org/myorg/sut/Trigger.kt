package org.myorg.sut

import aws.sdk.kotlin.services.dynamodb.DynamoDbClient
import com.amazonaws.services.lambda.runtime.Context
import com.amazonaws.services.lambda.runtime.RequestHandler
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent
import io.github.huherto.awsLambdaStream.EnvironmentConfig
import io.github.huherto.awsLambdaStream.FaultManager
import io.github.huherto.awsLambdaStream.PipelineAssembler
import io.github.huherto.awsLambdaStream.flavors.CorrelatePipeline
import io.github.huherto.awsLambdaStream.flavors.EvaluatePipeline
import io.github.huherto.awsLambdaStream.flavors.Pipeline
import io.github.huherto.awsLambdaStream.from.DynamodbAdapter
import io.github.huherto.awsLambdaStream.getDynamoDbClient
import io.github.huherto.awsLambdaStream.sinks.EventBridgePublishOptions
import kotlinx.coroutines.runBlocking
import mu.KotlinLogging

class Trigger constructor(
    private val envConfig: EnvironmentConfig,
    private var dynamoDbClient: DynamoDbClient? = null
): RequestHandler<DynamodbEvent, String> {

    private val logger = KotlinLogging.logger {  }

    private val correlatePipeline: Pipeline by lazy {
        if (dynamoDbClient == null) {
            dynamoDbClient = getDynamoDbClient(envConfig)
        }

        CorrelatePipeline(
            id = "corre1",
            envConfig = envConfig,
            unmarshall = { eventAsString : String -> jsonDecode(eventAsString)},
            correlationKey = { uow ->
                val event = uow.event as? TrackedUnitEvent
                event?.entity?.id ?: "no-correlation-key"
            },
            dynamoDbClient = dynamoDbClient,
            onEventClass = listOf(TrackedUnitEvent::class),
        )
    }

    private val evaluatePipeline: Pipeline by lazy {
        if (dynamoDbClient == null) {
            dynamoDbClient = getDynamoDbClient(envConfig)
        }

        EvaluatePipeline(
            id = "eval1",
            envConfig = envConfig,
            dynamoDbClient = dynamoDbClient,
            onEventClass = listOf(TrackedUnitEvent::class),
            eventBridgePublishOptions = EventBridgePublishOptions(envConfig),
        )
    }

    override fun handleRequest(ddbEvent: DynamodbEvent, context: Context) : String = runBlocking{

        val assembler = PipelineAssembler
            .builder()
            .addPipeline(correlatePipeline)
            .addPipeline(evaluatePipeline)
            .build()

        val headFlow = DynamodbAdapter().fromDynamoDB(FaultManager(), ddbEvent)
        logger.info { "Processing ${ddbEvent.records?.size} records" }
        assembler
            .assemble(headFlow, true)
            .collect {
                val eventClass = it.event?.javaClass?.simpleName ?: "null"
                logger.info { "collected event ${it.event?.id}, $eventClass" }
            }

        "Done"
    }
}