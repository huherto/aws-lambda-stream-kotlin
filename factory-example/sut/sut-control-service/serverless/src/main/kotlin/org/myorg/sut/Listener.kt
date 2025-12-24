package org.myorg.sut

import com.amazonaws.services.lambda.runtime.Context
import com.amazonaws.services.lambda.runtime.RequestHandler
import com.amazonaws.services.lambda.runtime.events.KinesisEvent
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
    val kinesisAdapter: KinesisAdapter<TrackedUnitEvent>) : RequestHandler<KinesisEvent, Void?>
{
    constructor() : this(getEventMicroStore(), getKinesisAdapter())

    override fun handleRequest(kinesisEvent: KinesisEvent, context: Context): Void? {
        val stream: Stream<UOW> = kinesisAdapter.fromKinesis(kinesisEvent)

        eventsMicrostore.save(stream, EventsMicrostore.SaveOptions(90))

        // When not using reportBatchItemFailures, return null to acknowledge the entire batch.
        return null
    }
}