package io.kopipes.aws.from

import com.amazonaws.services.lambda.runtime.events.SNSEvent
import io.kopipes.aws.EventCodec
import io.kopipes.aws.GlobalRegistry
import io.kopipes.aws.UnitOfWork
import io.kopipes.aws.faults.FaultManager
import io.kopipes.aws.metrics.PipelineMetrics
import io.kopipes.aws.metrics.Timer
import io.kopipes.aws.metrics.withMetrics
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.map

class SnsAdapter(
    private val faultManager: FaultManager = GlobalRegistry.faultManager(),
    private val eventCodec: EventCodec
) {
    fun fromSns(event: SNSEvent): Flow<UnitOfWork> {
        return event.records.orEmpty().asFlow()
            .map { record ->
                val timestamp = record.sns?.timestamp?.millis ?: System.currentTimeMillis()
                UnitOfWork(record = record).withMetrics(
                    PipelineMetrics(
                        timer = Timer(
                            start = timestamp,
                            last = timestamp
                        )
                    )
                )
            }
    }

    fun fromSnsEvent(event: SNSEvent): Flow<UnitOfWork> {
        with(faultManager) {
            return fromSns(event)
                .mapNotFaulty { uow ->
                    val record = uow.record as SNSEvent.SNSRecord
                    val sns = record.sns
                    val eventObj = eventCodec.decode(sns.message).let {
                        var updated = it
                        if (updated.id == null) updated = updated.copyEvent(id = sns.messageId)
                        if (updated.timestamp == null) updated = updated.copyEvent(timestamp = sns.timestamp?.millis)
                        updated
                    }
                    uow.copy(
                        event = eventObj
                    )
                }
        }
    }
}
