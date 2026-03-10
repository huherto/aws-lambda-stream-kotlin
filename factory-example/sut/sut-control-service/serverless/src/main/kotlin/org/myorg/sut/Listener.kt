package org.myorg.sut

import aws.sdk.kotlin.services.dynamodb.DynamoDbClient
import aws.sdk.kotlin.services.dynamodb.model.PutItemRequest
import aws.sdk.kotlin.services.dynamodb.model.PutItemResponse
import com.amazonaws.services.lambda.runtime.Context
import com.amazonaws.services.lambda.runtime.RequestHandler
import com.amazonaws.services.lambda.runtime.events.KinesisEvent
import io.github.huherto.awsLambdaStream.*
import kotlinx.coroutines.runBlocking
import mu.KotlinLogging
import org.slf4j.MarkerFactory
import java.nio.ByteBuffer

class MyKinesisAdapter : KinesisAdapter() {
    override fun decodePayload(payload: ByteBuffer?): TrackedUnitEvent {
        return sutJson.decodeFromString<TrackedUnitEvent>(utf8Decode(payload))
    }
}

class DynamoDBClientWrapper(val dynamoDBClient: DynamoDbClient) : DynamoDbClient by dynamoDBClient {

    private val logger = KotlinLogging.logger {  }

    override suspend fun putItem(input: PutItemRequest): PutItemResponse {
        val eventId = input.item?.get("pk")?.asS()
        logger.info { "Checking for poison event: $eventId" }
        if (eventId?.contains("poison") == true) {
            throw RuntimeException("Poison event detected $eventId")
        }
        logger.info { "Putting item: $eventId" }
        return dynamoDBClient.putItem(input)
    }
}

class Listener(
    private val initialAdapter: KinesisAdapter? = null,
    val envConfig: EnvironmentConfig = EnvironmentConfig(),
    private var dynamoDbClient: DynamoDbClient? = null
) : RequestHandler<KinesisEvent, Void?> {

    private val logger = KotlinLogging.logger {  }

    private val kinesisAdapter: KinesisAdapter by lazy {
        initialAdapter ?: run {
            logger.info("Getting Kinesis adapter")
            MyKinesisAdapter()
        }
    }

    private val assembler: PipelineAssembler by lazy {

        if (dynamoDbClient == null) {
            dynamoDbClient = DynamoDBClientWrapper(getDynamoDbClient(envConfig))
        }

        val pipeline =
            CollectPipeline.Builder("col1")
                .dynamoDbClient(dynamoDbClient)
                .onEventClass(listOf(Event::class))
                .envConfig(envConfig)
                .build()
        val assembler = PipelineAssembler
            .builder()
                .addPipeline(pipeline)
                .build()
        assembler
    }

    override fun handleRequest(kinesisEvent: KinesisEvent, context: Context): Void? = runBlocking{
        // Is this try..catch even needed any more?
        try {
            val headFlow = kinesisAdapter.fromKinesis(kinesisEvent)
            assembler
                .assemble(headFlow, true)
                .collect { logger.info { "collected " + it.event?.id} }
        } catch (e: Throwable) {
            logger.error(MarkerFactory.getMarker("FATAL"), "Exception in lambda handler", e)
        }
        null
    }
}