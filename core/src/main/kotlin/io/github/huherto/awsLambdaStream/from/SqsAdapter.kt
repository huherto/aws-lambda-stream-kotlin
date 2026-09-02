package io.github.huherto.awsLambdaStream.from

import com.amazonaws.services.lambda.runtime.events.SQSEvent
import io.github.huherto.awsLambdaStream.EventCodec
import io.github.huherto.awsLambdaStream.GlobalRegistry
import io.github.huherto.awsLambdaStream.GlobalRegistry.envConfig
import io.github.huherto.awsLambdaStream.UnitOfWork
import io.github.huherto.awsLambdaStream.faults.FaultManager
import io.github.huherto.awsLambdaStream.filters.outSkip
import io.github.huherto.awsLambdaStream.metrics.PipelineMetrics
import io.github.huherto.awsLambdaStream.metrics.Timer
import io.github.huherto.awsLambdaStream.metrics.withMetrics
import io.github.huherto.awsLambdaStream.queries.ClaimCheckRedeemer
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

