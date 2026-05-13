package io.github.huherto.awsLambdaStream.from

import com.amazonaws.services.lambda.runtime.events.KinesisEvent
import io.github.huherto.awsLambdaStream.Event
import io.github.huherto.awsLambdaStream.FaultManager
import io.github.huherto.awsLambdaStream.UnitOfWork
import kotlinx.coroutines.flow.*
import java.nio.ByteBuffer

// TODO - This class needs unit tests.
//
abstract class KinesisAdapter(private val faultManager: FaultManager) {

    fun  fromKinesis(kinesisEvent: KinesisEvent): Flow<UnitOfWork> {
        if (kinesisEvent.records.isNullOrEmpty()) {
            return emptyFlow()
        }
        with(faultManager) {
            return kinesisEvent.records.asFlow()
                .mapNotNull { record ->
                    UnitOfWork().copy(
                        record = record,
                    )
                }.mapNotFaulty { uow ->
                    val record = uow.record as KinesisEvent.KinesisEventRecord
                    val payload = record.kinesis?.data

                    val event: Event = decodePayload(payload)
                    if (event.id == null) {
                        event.id = (record.eventID)
                    }
                    uow.copy( event = event, sequenceNumber = record.kinesis?.sequenceNumber)
                }.map { uow ->
                    // TODO: call claim_check processing
                    uow
                }
        }

    }

    abstract fun decodePayload(payload : ByteBuffer?) : Event

}