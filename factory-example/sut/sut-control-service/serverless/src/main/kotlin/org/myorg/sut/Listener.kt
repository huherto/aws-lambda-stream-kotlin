package org.myorg.sut

import com.amazonaws.services.lambda.runtime.Context
import com.amazonaws.services.lambda.runtime.RequestHandler
import com.amazonaws.services.lambda.runtime.events.KinesisEvent
import io.github.huherto.`aws-lambda-stream`.*
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.slf4j.MarkerFactory
import java.nio.ByteBuffer
import java.time.Clock

typealias UOW = UnitOfWork<TrackedUnitEvent>

class MyKinesisAdapter : KinesisAdapter<TrackedUnitEvent>() {
    override fun decodePayload(payload: ByteBuffer?): TrackedUnitEvent {
        return sutJson.decodeFromString<TrackedUnitEvent>(utf8Decode(payload))
    }
}

class Listener(
    private val initialStore: EventsMicrostore<TrackedUnitEvent>? = null,
    private val initialAdapter: KinesisAdapter<TrackedUnitEvent>? = null
) : RequestHandler<KinesisEvent, Void?> {

    // AWS Lambda requires a no-arg constructor

    private val eventsMicrostore: EventsMicrostore<TrackedUnitEvent> by lazy {
        initialStore ?: run {
            println("Getting DynamoDB client")
            val client = getDynamoDbClient()
                ?: throw IllegalStateException("DynamoDB client is not configured.")

            println("Using DynamoDB client: $client")
            EventsMicrostoreImpl(
                client,
                Clock.systemDefaultZone(),
                EnvironmentConfig()
            )
        }
    }

    private val kinesisAdapter: KinesisAdapter<TrackedUnitEvent> by lazy {
        initialAdapter ?: run {
            println("Getting Kinesis adapter")
            MyKinesisAdapter()
        }
    }

    override fun handleRequest(kinesisEvent: KinesisEvent, context: Context): Void? {
        val logger: Logger = LoggerFactory.getLogger(Listener::class.java)!!
        try {
            logger.info("Handling request: {}", kinesisEvent)

            // properties are now non-nullable and initialized on first access
            val stream = kinesisAdapter.fromKinesis(kinesisEvent)
            eventsMicrostore.save(stream, EventsMicrostore.SaveOptions(90))

        } catch (e: Throwable) {
            logger.error(MarkerFactory.getMarker("FATAL"), "Exception in lambda handler", e)
        }
        return null
    }
}