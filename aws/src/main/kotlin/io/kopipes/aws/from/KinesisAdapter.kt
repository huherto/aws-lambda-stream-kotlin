package io.kopipes.aws.from

import com.amazonaws.services.lambda.runtime.events.KinesisEvent
import io.kopipes.aws.queries.ClaimCheckRedeemer
import io.kopipes.core.Event
import io.kopipes.core.EventCodec
import io.kopipes.core.GlobalRegistry
import io.kopipes.core.GlobalRegistry.envConfig
import io.kopipes.core.UnitOfWork
import io.kopipes.core.faults.FaultManager
import io.kopipes.core.metrics.PipelineMetrics
import io.kopipes.core.metrics.Timer
import io.kopipes.core.metrics.withMetrics
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.emptyFlow
import kotlinx.coroutines.flow.mapNotNull

class KinesisAdapter(
    private val faultManager: FaultManager = GlobalRegistry.faultManager(),
    private val eventCodec: EventCodec,
    private val claimCheckRedeemer: ClaimCheckRedeemer? = null
) {

    constructor(eventCodec: EventCodec) : this(GlobalRegistry.faultManager(), eventCodec)

    fun fromKinesis(kinesisEvent: KinesisEvent): Flow<UnitOfWork> {
        if (kinesisEvent.records.isNullOrEmpty()) {
            return emptyFlow()
        }
        with(faultManager) {
            val batchUtilization = kinesisEvent.records.size.toDouble() / (envConfig().batchSize() ?: 100)
            return kinesisEvent.records.asFlow()
                .mapNotNull { record ->
                    val timestamp = record.kinesis?.approximateArrivalTimestamp?.time ?: System.currentTimeMillis()
                    UnitOfWork().copy(
                        record = record,
                    ).withMetrics(
                        PipelineMetrics(
                            timer = Timer(
                                start = timestamp,
                                last = timestamp
                            )
                        ).gauge("stream.batch.utilization", batchUtilization)
                    )
                }.mapNotFaulty { uow ->
                    val record = uow.record as KinesisEvent.KinesisEventRecord
                    val payload = record.kinesis?.data

                    val event: Event = eventCodec.decode(payload).let {
                        if (it.id == null) it.copyEvent(id = record.eventID) else it
                    }
                    uow.copy(event = event, sequenceNumber = record.kinesis?.sequenceNumber)
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
