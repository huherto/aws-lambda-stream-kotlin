package org.myorg.sut

import com.amazonaws.services.lambda.runtime.Context
import com.amazonaws.services.lambda.runtime.RequestHandler
import com.amazonaws.services.lambda.runtime.events.KinesisEvent
import java.util.stream.Stream
import kotlin.time.Clock
import kotlin.time.ExperimentalTime

typealias UOW = UnitOfWork<TrackedUnit>

@OptIn(ExperimentalTime::class)
fun getEventMicroSore() : EventsMicrostore<TrackedUnit> {
    return EventsMicrostoreImpl<TrackedUnit>(getDynamoDbClient()!!, Clock.System, EnvironmentConfig())
}

@OptIn(ExperimentalTime::class)
class KinesisHandler(
    val eventsMicrostore: EventsMicrostore<TrackedUnit>,
    val kinesisAdapter: KinesisAdapter<TrackedUnit>) : RequestHandler<KinesisEvent, Void?>
{
    constructor() : this(getEventMicroSore(), KinesisAdapter<TrackedUnit>())

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