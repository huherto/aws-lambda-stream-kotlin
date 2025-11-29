package org.myorg.sut

import com.amazonaws.services.lambda.runtime.Context
import com.amazonaws.services.lambda.runtime.RequestHandler
import com.amazonaws.services.lambda.runtime.events.KinesisEvent
import java.time.Clock
import java.util.stream.Stream

typealias UOW = UnitOfWork<TrackedUnit>

fun getEventMicroStore() : EventsMicrostore<TrackedUnit> {
    return EventsMicrostoreImpl<TrackedUnit>(getDynamoDbClient()!!, Clock.systemDefaultZone(), EnvironmentConfig())
}

class Listener(
    val eventsMicrostore: EventsMicrostore<TrackedUnit>,
    val kinesisAdapter: KinesisAdapter<TrackedUnit>) : RequestHandler<KinesisEvent, Void?>
{
    constructor() : this(getEventMicroStore(), KinesisAdapter<TrackedUnit>())

    override fun handleRequest(kinesisEvent: KinesisEvent, context: Context): Void? {
        val stream: Stream<UOW> =
            kinesisAdapter.fromKinesis(kinesisEvent)

        eventsMicrostore.save(
            stream,
            EventsMicrostore.SaveOptions(90))

        // When not using reportBatchItemFailures, return null to acknowledge the entire batch.
        return null
    }

}