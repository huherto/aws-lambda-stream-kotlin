package io.github.huherto.awsLambdaStream.from

import com.amazonaws.services.lambda.runtime.events.SNSEvent
import io.github.huherto.awsLambdaStream.EventCodec
import io.github.huherto.awsLambdaStream.FaultManager
import io.github.huherto.awsLambdaStream.GlobalRegistry
import io.github.huherto.awsLambdaStream.UnitOfWork
import io.github.huherto.awsLambdaStream.metrics.PipelineMetrics
import io.github.huherto.awsLambdaStream.metrics.Timer
import io.github.huherto.awsLambdaStream.metrics.withMetrics
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
