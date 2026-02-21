package org.myorg.sut

import com.amazonaws.services.lambda.runtime.Context
import com.amazonaws.services.lambda.runtime.RequestHandler
import com.amazonaws.services.lambda.runtime.events.KinesisEvent
import io.github.huherto.`aws-lambda-stream`.*
import kotlinx.coroutines.runBlocking
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.slf4j.MarkerFactory
import java.nio.ByteBuffer
import java.time.Clock

class MyKinesisAdapter : KinesisAdapter<TrackedUnitEvent>() {
    override fun decodePayload(payload: ByteBuffer?): TrackedUnitEvent {
        return sutJson.decodeFromString<TrackedUnitEvent>(utf8Decode(payload))
    }
}

class Listener(
    private val initialStore: EventsMicrostore<TrackedUnitEvent>? = null,
    private val initialAdapter: KinesisAdapter<TrackedUnitEvent>? = null
) : RequestHandler<KinesisEvent, Void?> {

    private val logger: Logger = LoggerFactory.getLogger(Listener::class.java)!!

    private val envConfig = EnvironmentConfig()

    private val eventsMicrostore: EventsMicrostore<TrackedUnitEvent> by lazy {
        initialStore ?: run {
            logger.info("Getting DynamoDB client")
            val client = getDynamoDbClient(envConfig)
            logger.info("Using DynamoDB client: $client")
            EventsMicrostoreImpl(
                client,
                Clock.systemDefaultZone(),
                envConfig
            )
        }
    }

    private val kinesisAdapter: KinesisAdapter<TrackedUnitEvent> by lazy {
        initialAdapter ?: run {
            logger.info("Getting Kinesis adapter")
            MyKinesisAdapter()
        }
    }

    override fun handleRequest(kinesisEvent: KinesisEvent, context: Context): Void? = runBlocking{

        try {
            logger.info("Handling request: {}", kinesisEvent)
            val flow = kinesisAdapter.fromKinesis(kinesisEvent)
                .filterEventTypes(ShipmentCreatedEvent::class, ShipmentPickedUpEvent::class)
            eventsMicrostore.save(flow, EventsMicrostore.SaveOptions(90))
        } catch (e: Throwable) {
            logger.error(MarkerFactory.getMarker("FATAL"), "Exception in lambda handler", e)
        }
        null
    }
}