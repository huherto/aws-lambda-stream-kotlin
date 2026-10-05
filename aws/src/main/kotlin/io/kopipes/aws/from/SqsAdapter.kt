package io.kopipes.aws.from

import com.amazonaws.services.lambda.runtime.events.SQSEvent
import io.kopipes.aws.queries.ClaimCheckRedeemer
import io.kopipes.core.EventCodec
import io.kopipes.core.GlobalRegistry
import io.kopipes.core.GlobalRegistry.envConfig
import io.kopipes.core.UnitOfWork
import io.kopipes.core.faults.FaultManager
import io.kopipes.core.filters.outSkip
import io.kopipes.core.metrics.PipelineMetrics
import io.kopipes.core.metrics.Timer
import io.kopipes.core.metrics.withMetrics
import kotlinx.coroutines.flow.*

class SqsAdapter(
    private val faultManager: FaultManager = GlobalRegistry.faultManager(),
    private val eventCodec: EventCodec,
    private val claimCheckRedeemer: ClaimCheckRedeemer? = null
) {

    fun fromSqs(sqsEvent: SQSEvent): Flow<UnitOfWork> {
        val records = sqsEvent.records

        if (records.isNullOrEmpty()) {
            return emptyFlow()
        }

        val batchUtilization = records.size.toDouble() / (envConfig().batchSize() ?: 100)
        return records.asFlow()
            .map { record ->
                val timestamp = record.attributes?.get("SentTimestamp")?.toLong() ?: System.currentTimeMillis()
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
            }
    }

    fun fromSqsEvent(sqsEvent: SQSEvent): Flow<UnitOfWork> {
        val records = sqsEvent.records

        if (records.isNullOrEmpty()) {
            return emptyFlow()
        }

        with(faultManager) {
            val batchUtilization = records.size.toDouble() / (envConfig().batchSize() ?: 100)
            return records.asFlow()
                .mapNotNull { record ->
                    val timestamp = record.attributes?.get("SentTimestamp")?.toLong() ?: System.currentTimeMillis()
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
                }
                .mapNotFaulty { uow ->
                    val record = uow.record as SQSEvent.SQSMessage

                    val event = eventCodec.decode(record.body).let {
                        if (it.id == null) it.copyEvent(id = record.messageId) else it
                    }

                    uow.copy(
                        event = event,
                    )
                }
                .filter { uow ->
                    outSkip(uow)
                }
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
