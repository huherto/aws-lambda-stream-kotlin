package io.github.huherto.`aws-lambda-stream`

import com.amazonaws.services.lambda.runtime.events.KinesisEvent
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.map
import java.nio.ByteBuffer
import java.util.stream.Stream

abstract class KinesisAdapter<E : Event> {

    fun  fromKinesis(kinesisEvent: KinesisEvent): Flow<UnitOfWork<E>> {
        return kinesisEvent.records.asFlow()
            .map{ record ->
                UnitOfWork<E>().apply {
                    this.record = record
                }
            }.map { uow ->
                val record = uow.record as KinesisEvent.KinesisEventRecord
                val payload = record.kinesis?.data

                val event: E = decodePayload(payload)
                if (event.id == null) {
                    event.id = (record.eventID)
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