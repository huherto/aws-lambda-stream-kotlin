package io.github.huherto.awsLambdaStream.from

import com.amazonaws.services.lambda.runtime.events.KinesisEvent
import io.github.huherto.awsLambdaStream.Event
import io.github.huherto.awsLambdaStream.FaultManager
import io.github.huherto.awsLambdaStream.UnitOfWork
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.mapNotNull
import java.nio.ByteBuffer

abstract class KinesisAdapter {

    fun  fromKinesis(faultManager: FaultManager, kinesisEvent: KinesisEvent): Flow<UnitOfWork> {

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
                    uow.copy( event = event)
                }.map { uow ->
                    // TODO: call claim_check processing
                    uow
                }
        }

    }

    abstract fun decodePayload(payload : ByteBuffer?) : Event

}