package org.myorg.sut

import aws.sdk.kotlin.services.dynamodb.DynamoDbClient
import com.amazonaws.services.lambda.runtime.Context
import com.amazonaws.services.lambda.runtime.RequestHandler
import com.amazonaws.services.lambda.runtime.events.KinesisEvent
import io.github.huherto.awsLambdaStream.*
import io.github.huherto.awsLambdaStream.flavors.CollectPipeline
import io.github.huherto.awsLambdaStream.flavors.Pipeline
import io.github.huherto.awsLambdaStream.from.KinesisAdapter
import kotlinx.coroutines.runBlocking
import mu.KotlinLogging
import java.nio.ByteBuffer

class MyKinesisAdapter : KinesisAdapter() {
    override fun decodePayload(payload: ByteBuffer?): TrackedUnitEvent {
        return sutJson.decodeFromString<TrackedUnitEvent>(utf8Decode(payload))
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

    private val pipeline: Pipeline by lazy {
        if (dynamoDbClient == null) {
            dynamoDbClient = DynamoDBClientWrapper(getDynamoDbClient(envConfig))
        }

        CollectPipeline.Builder("collect1")
            .dynamoDbClient(dynamoDbClient)
            .onEventClass(listOf(Event::class))
            .envConfig(envConfig)
            .build()
    }

    override fun handleRequest(kinesisEvent: KinesisEvent, context: Context): Void? = runBlocking{
        val assembler = PipelineAssembler
            .builder()
            .addPipeline(pipeline)
            .build()

        val headFlow = kinesisAdapter
            .fromKinesis(assembler.getFaultManager(), kinesisEvent)

        assembler
            .assemble(headFlow, true)
            .collect { logger.info { "collected " + it.event?.id} }
        null
    }
}