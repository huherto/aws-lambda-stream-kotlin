package org.myorg.sut

import aws.sdk.kotlin.services.dynamodb.DynamoDbClient
import com.amazonaws.services.lambda.runtime.Context
import com.amazonaws.services.lambda.runtime.RequestHandler
import com.amazonaws.services.lambda.runtime.events.KinesisEvent
import io.github.huherto.awsLambdaStream.*
import kotlinx.coroutines.runBlocking
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.slf4j.MarkerFactory
import java.nio.ByteBuffer

class MyKinesisAdapter : KinesisAdapter() {
    override fun decodePayload(payload: ByteBuffer?): TrackedUnitEvent {
        return sutJson.decodeFromString<TrackedUnitEvent>(utf8Decode(payload))
    }
}

class Listener(
    private val initialStore: EventsMicrostore? = null,
    private val initialAdapter: KinesisAdapter? = null,
    val envConfig: EnvironmentConfig = EnvironmentConfig(),
    val dynamoDbClient: DynamoDbClient = getDynamoDbClient(envConfig),
) : RequestHandler<KinesisEvent, Void?> {

    private val logger: Logger = LoggerFactory.getLogger(Listener::class.java)!!

    private val kinesisAdapter: KinesisAdapter by lazy {
        initialAdapter ?: run {
            logger.info("Getting Kinesis adapter")
            MyKinesisAdapter()
        }
    }

    private val assembler: PipelineAssembler by lazy {
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
        
        try {
            logger.info("Handling request: {}", kinesisEvent)
            val headFlow = kinesisAdapter.fromKinesis(kinesisEvent)
            val completeFlow = assembler
                .assemble(headFlow, false)
            completeFlow.collect { println(">" + it.event.toString().replace("\\s".toRegex(), "")) }
        } catch (e: Throwable) {
            logger.error(MarkerFactory.getMarker("FATAL"), "Exception in lambda handler", e)
        }
        null
    }
}