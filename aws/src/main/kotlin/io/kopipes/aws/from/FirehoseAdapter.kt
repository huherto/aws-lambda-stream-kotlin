package io.kopipes.aws.from

import com.amazonaws.services.lambda.runtime.events.KinesisFirehoseEvent
import io.kopipes.aws.queries.ClaimCheckRedeemer
import io.kopipes.core.EventCodec
import io.kopipes.core.GlobalRegistry
import io.kopipes.core.UnitOfWork
import io.kopipes.core.faults.FaultManager
import io.kopipes.core.filters.outSkip
import kotlinx.coroutines.flow.*

class FirehoseAdapter(
    private val faultManager: FaultManager = GlobalRegistry.faultManager(),
    private val eventCodec: EventCodec,
    private val claimCheckRedeemer: ClaimCheckRedeemer? = null
) {
    fun fromFirehose(event: KinesisFirehoseEvent): Flow<UnitOfWork> {
        val records = event.records
        if (records.isNullOrEmpty()) {
            return emptyFlow()
        }

        with(faultManager) {
            return records.asFlow()
                .map { record ->
                    UnitOfWork(record = record).withExtension(FirehoseExtension(record.recordId))
                }
                .mapNotFaulty { uow ->
                    val record = uow.record as KinesisFirehoseEvent.Record
                    val data = record.data
                    val payload = Charsets.UTF_8.decode(data).toString()

                    val eventObj = eventCodec.decode(payload).let {
                        if (it.id == null) it.copyEvent(id = record.recordId) else it
                    }
                    uow.copy(
                        event = eventObj,
                        timestamp = record.approximateArrivalTimestamp?.toString()
                    )
                }
                .filter { uow -> outSkip(uow) }
                .let { flow ->
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

data class FirehoseExtension(val recordId: String)
