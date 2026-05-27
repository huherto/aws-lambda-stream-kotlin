package io.github.huherto.awsLambdaStream.from

import com.amazonaws.services.lambda.runtime.events.KinesisEvent
import io.github.huherto.awsLambdaStream.Event
import io.github.huherto.awsLambdaStream.EventCodec
import io.github.huherto.awsLambdaStream.FaultManager
import io.github.huherto.awsLambdaStream.UnitOfWork
import io.github.huherto.awsLambdaStream.queries.ClaimCheckRedeemer
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.emptyFlow
import kotlinx.coroutines.flow.mapNotNull

class KinesisAdapter(
    private val faultManager: FaultManager,
    private val eventCodec: EventCodec,
    private val claimCheckRedeemer: ClaimCheckRedeemer? = null
) {

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

                    val event: Event = eventCodec.decode(payload)
                    if (event.id == null) {
                        event.id = (record.eventID)
                    }
                    uow.copy( event = event, sequenceNumber = record.kinesis?.sequenceNumber)
                }.let { flow ->
                    if (claimCheckRedeemer != null) {
                        with(claimCheckRedeemer) {
                            flow.redeemClaimCheck()
                        }
                    } else {
                        flow
                    }
                }
        }

    }

}