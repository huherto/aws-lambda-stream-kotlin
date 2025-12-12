package org.myorg.sut

import com.amazonaws.services.lambda.runtime.events.KinesisEvent
import kotlinx.serialization.json.Json
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.util.stream.Stream

open class KinesisAdapter<E : Event> {

    fun  fromKinesis(kinesisEvent: KinesisEvent): Stream<UnitOfWork<E>> {
        return kinesisEvent.records.map { record ->
            UnitOfWork<E>().apply {
                this.record = record
            }
        }.stream().map { uow ->
            val payload = uow.record?.kinesis?.data
            val event: Event = decodePayload(payload)
            if (uow.record != null && event.entity != null) {
                    event.id = (uow.record?.eventID)
            }
            uow.event = event
            uow
        }.map { uow ->
            // TODO: call claim_check processing
            uow;
        }
    }

     fun utf8Decode(bb : ByteBuffer?) : String {
         if (bb == null) return ""
        return StandardCharsets.UTF_8.decode(bb).toString()
    }

    inline fun <reified E> decodePayload(payload : ByteBuffer?) : E {
        return Json.decodeFromString<E>(utf8Decode(payload))
    }
}