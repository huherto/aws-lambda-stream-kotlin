package org.myorg.sut

import com.amazonaws.services.lambda.runtime.events.KinesisEvent
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.util.stream.Stream

abstract class KinesisAdapter<E : Event> {

    fun  fromKinesis(kinesisEvent: KinesisEvent): Stream<UnitOfWork<E>> {
        return kinesisEvent.records.map { record ->
            UnitOfWork<E>().apply {
                this.record = record
            }
        }.stream().map { uow ->
            val payload = uow.record?.kinesis?.data
            val event: E = decodePayload(payload)
            if (event.id == null && uow.record != null) {
                    event.id = (uow.record?.eventID)
            }
            uow.event = event
            uow
        }.map { uow ->
            // TODO: call claim_check processing
            uow
        }
    }

     fun utf8Decode(bb : ByteBuffer?) : String {
         if (bb == null) return ""
        return StandardCharsets.UTF_8.decode(bb).toString()
    }

    abstract fun decodePayload(payload : ByteBuffer?) : E

}