package io.github.huherto.`aws-lambda-stream`

import com.amazonaws.services.lambda.runtime.events.KinesisEvent
import java.nio.ByteBuffer
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

    abstract fun decodePayload(payload : ByteBuffer?) : E

}