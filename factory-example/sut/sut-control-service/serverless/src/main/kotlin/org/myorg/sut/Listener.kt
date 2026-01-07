package org.myorg.sut

import com.amazonaws.services.lambda.runtime.Context
import com.amazonaws.services.lambda.runtime.RequestHandler
import com.amazonaws.services.lambda.runtime.events.KinesisEvent
import io.github.huherto.`aws-lambda-stream`.*
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.slf4j.MarkerFactory
import software.amazon.lambda.powertools.logging.PowertoolsLogging
import software.amazon.lambda.powertools.logging.argument.StructuredArguments.entry
import java.nio.ByteBuffer
import java.time.Clock
import java.util.stream.Stream

typealias UOW = UnitOfWork<TrackedUnitEvent>

fun getEventMicroStore(): EventsMicrostore<TrackedUnitEvent> {
    val client = getDynamoDbClient()
        ?: throw IllegalStateException(
            "DynamoDB client is not configured. " +
                "Ensure environment/region/endpoint is set, or inject a fake EventsMicrostore in tests."
        )

    return EventsMicrostoreImpl(
        client,
        Clock.systemDefaultZone(),
        EnvironmentConfig()
    )
}

class MyKinesisAdapter : KinesisAdapter<TrackedUnitEvent>() {
    override fun decodePayload(payload: ByteBuffer?): TrackedUnitEvent {
        return sutJson.decodeFromString<TrackedUnitEvent>(utf8Decode(payload))
    }
}

fun getKinesisAdapter(): KinesisAdapter<TrackedUnitEvent> {
    return MyKinesisAdapter()
}

class Listener(
    val eventsMicrostore: EventsMicrostore<TrackedUnitEvent>,
    val kinesisAdapter: KinesisAdapter<TrackedUnitEvent>
) : RequestHandler<KinesisEvent, Void?>
{
    constructor() : this(getEventMicroStore(), getKinesisAdapter())

    override fun handleRequest(kinesisEvent: KinesisEvent, context: Context): Void? {
        val logger: Logger = LoggerFactory.getLogger(Listener::class.java)!!

        return PowertoolsLogging.withLogging(context, {

            try {
                logger.info("Handling request: {}", entry("kinesisEvent", kinesisEvent))
                val stream: Stream<UOW> = kinesisAdapter.fromKinesis(kinesisEvent)

                eventsMicrostore.save(stream, EventsMicrostore.SaveOptions(90))

            } catch (e: Throwable) {
                logger.error(MarkerFactory.getMarker("FATAL"), "Exception in lambda handler", e)
            }

            // When not using reportBatchItemFailures, return null to acknowledge the entire batch.
            null
        })
    }
}