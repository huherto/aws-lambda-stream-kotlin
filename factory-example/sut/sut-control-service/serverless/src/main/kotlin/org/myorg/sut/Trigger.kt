package org.myorg.sut

import aws.sdk.kotlin.services.dynamodb.DynamoDbClient
import com.amazonaws.services.lambda.runtime.Context
import com.amazonaws.services.lambda.runtime.RequestHandler
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent
import io.github.huherto.awsLambdaStream.*
import io.github.huherto.awsLambdaStream.flavors.CorrelatePipeline
import io.github.huherto.awsLambdaStream.flavors.Pipeline
import io.github.huherto.awsLambdaStream.from.DynamodbAdapter
import kotlinx.coroutines.runBlocking
import mu.KotlinLogging

class Trigger : RequestHandler<DynamodbEvent, String> {

    private val logger = KotlinLogging.logger {  }

    val envConfig: EnvironmentConfig = EnvironmentConfig()

    private var dynamoDbClient: DynamoDbClient? = null

    private val pipeline: Pipeline by lazy {
        if (dynamoDbClient == null) {
            dynamoDbClient = DynamoDBClientWrapper(getDynamoDbClient(envConfig))
        }

        CorrelatePipeline(
            id = "corr1",
            correlationKey = { "my-correlation-key" },
            dynamoDbClient = dynamoDbClient,
            onEventClass = listOf(Event::class)
            )
    }

    override fun handleRequest(ddbEvent: DynamodbEvent, context: Context) : String = runBlocking{

        val assembler = PipelineAssembler
            .builder()
            .addPipeline(pipeline)
            .build()

        val headFlow = DynamodbAdapter().fromDynamoDB(FaultManager(), ddbEvent)

        assembler
            .assemble(headFlow, true)
            .collect {
                val eventClass = it.event?.javaClass?.simpleName ?: "null"
                logger.info { "collected ${it.event?.id}, $eventClass" }
            }

        "Done"
    }
}