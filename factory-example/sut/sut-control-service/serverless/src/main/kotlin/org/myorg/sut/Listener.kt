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
import java.util.stream.Stream

typealias UOW = UnitOfWork<TrackedUnitEvent>

class MyKinesisAdapter : KinesisAdapter<TrackedUnitEvent>() {
    override fun decodePayload(payload: ByteBuffer?): TrackedUnitEvent {
        return sutJson.decodeFromString<TrackedUnitEvent>(utf8Decode(payload))
    }
}

class Listener : RequestHandler<KinesisEvent, Void?>
{
    private var eventsMicrostore: EventsMicrostore<TrackedUnitEvent>? = null
    private var kinesisAdapter: KinesisAdapter<TrackedUnitEvent>? = null

    constructor(eventsMicrostore: EventsMicrostore<TrackedUnitEvent>, kinesisAdapter: KinesisAdapter<TrackedUnitEvent>) {
        this.eventsMicrostore = eventsMicrostore
        this.kinesisAdapter = kinesisAdapter
    }

    override fun handleRequest(kinesisEvent: KinesisEvent, context: Context): Void? {
        val logger: Logger = LoggerFactory.getLogger(Listener::class.java)!!
        try {
            logger.info("Handling request: {}",  kinesisEvent)
            val stream: Stream<UOW> = getKinesisAdapter().fromKinesis(kinesisEvent)

            getEventMicroStore().save(stream, EventsMicrostore.SaveOptions(90))

        } catch (e: Throwable) {
            logger.error(MarkerFactory.getMarker("FATAL"), "Exception in lambda handler", e)
        }

        // When not using reportBatchItemFailures, return null to acknowledge the entire batch.
        return null
    }

    fun getKinesisAdapter(): KinesisAdapter<TrackedUnitEvent> {
        println("Getting Kinesis adapter")
        return MyKinesisAdapter()
    }

    fun getEventMicroStore(): EventsMicrostore<TrackedUnitEvent> {
        if (eventsMicrostore != null) {
            return eventsMicrostore!!
        }

        println("Getting DynamoDB client")
        val client = getDynamoDbClient()
        ?: throw IllegalStateException(
            "DynamoDB client is not configured. " +
                "Ensure environment/region/endpoint is set, or inject a fake EventsMicrostore in tests."
        )

        println("Using DynamoDB client: $client")
        eventsMicrostore = EventsMicrostoreImpl(
            client,
            Clock.systemDefaultZone(),
            EnvironmentConfig()
        )
        return  eventsMicrostore!!
    }
}