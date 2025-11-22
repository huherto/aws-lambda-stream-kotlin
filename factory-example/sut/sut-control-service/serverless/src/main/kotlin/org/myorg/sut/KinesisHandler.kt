package org.myorg.sut

import com.amazonaws.services.lambda.runtime.Context
import com.amazonaws.services.lambda.runtime.RequestHandler
import com.amazonaws.services.lambda.runtime.events.KinesisEvent
import kotlinx.serialization.json.Json
import java.nio.charset.StandardCharsets
import java.util.stream.Stream
import kotlin.time.ExperimentalTime

class TrackedUnit : Thing {
    var id: String? = null
    var timestamp: String? = null

    override fun id(): String? {
        return id;
    }

    override fun setId(id: String?) {
        this.id = id;
    }

    override fun timestamp(): String? {
       return timestamp;
    }
}

@OptIn(ExperimentalTime::class)
class KinesisHandler(
    val eventsMicrostore: EventsMicrostore<TrackedUnit>,
    val kinesisAdapter: KinesisAdapter<TrackedUnit>) : RequestHandler<KinesisEvent, Void?>
{
    constructor() : this(EventsMicrostoreImpl<TrackedUnit>(), KinesisAdapter<TrackedUnit>())

    override fun handleRequest(kinesisEvent: KinesisEvent, context: Context): Void? {
        val stream = kinesisAdapter.fromKinesis(kinesisEvent)
        eventsMicrostore.save(
            kinesisAdapter.fromKinesis(kinesisEvent),
            EventsMicrostore.SaveOptions(90))

        // When not using reportBatchItemFailures, return null to acknowledge the entire batch.
        return null
    }

}