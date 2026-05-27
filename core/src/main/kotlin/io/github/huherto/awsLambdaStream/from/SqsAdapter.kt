package io.github.huherto.awsLambdaStream.from

import com.amazonaws.services.lambda.runtime.events.SQSEvent
import io.github.huherto.awsLambdaStream.EventCodec
import io.github.huherto.awsLambdaStream.FaultManager
import io.github.huherto.awsLambdaStream.UnitOfWork
import io.github.huherto.awsLambdaStream.filters.outSkip
import io.github.huherto.awsLambdaStream.queries.ClaimCheckRedeemer
import kotlinx.coroutines.flow.*

class SqsAdapter(
    private val faultManager: FaultManager,
    private val eventCodec: EventCodec,
    private val claimCheckRedeemer: ClaimCheckRedeemer? = null
) {

    /**
     * Intended for intra-service SQS messages, as opposed to consuming
     * inter-service events.
     */
    fun fromSqs(sqsEvent: SQSEvent): Flow<UnitOfWork> {
        val records = sqsEvent.records

        if (records.isNullOrEmpty()) {
            return emptyFlow()
        }

        return records.asFlow()
            .map { record ->
                UnitOfWork().copy(
                    record = record,
                )
            }
            .map { uow ->
                // TODO: call adorn metrics
                uow
            }
    }

    fun fromSqsEvent(sqsEvent: SQSEvent): Flow<UnitOfWork> {
        val records = sqsEvent.records

        if (records.isNullOrEmpty()) {
            return emptyFlow()
        }

        with(faultManager) {
            return records.asFlow()
                .mapNotNull { record ->
                    UnitOfWork().copy(
                        record = record,
                    )
                }
                .mapNotFaulty { uow ->
                    val record = uow.record as SQSEvent.SQSMessage

                    val event = eventCodec.decode(record.body)
                    if (event.id == null) {
                        event.id = record.messageId
                    }

                    uow.copy(
                        event = event,
                    )
                }
                .map { uow ->
                    // TODO: adorn metrics.
                    uow
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

