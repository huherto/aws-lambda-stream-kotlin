package io.github.huherto.awsLambdaStream.from

import com.amazonaws.services.lambda.runtime.events.ScheduledEvent
import io.github.huherto.awsLambdaStream.*
import io.github.huherto.awsLambdaStream.metrics.PipelineMetrics
import io.github.huherto.awsLambdaStream.metrics.Timer
import io.github.huherto.awsLambdaStream.metrics.withMetrics
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flowOf

class EventBridgeAdapter(
    private val faultManager: FaultManager = GlobalRegistry.faultManager(),
    private val eventCodec: EventCodec
) {

    fun fromEventBridge(event: ScheduledEvent): Flow<UnitOfWork> {
        val timestamp = event.time?.millis ?: System.currentTimeMillis()
        with(faultManager) {
            return flowOf(
                UnitOfWork(record = event).withMetrics(
                    PipelineMetrics(
                        timer = Timer(
                            start = timestamp,
                            last = timestamp
                        )
                    )
                )
            )
                .mapNotFaulty { uow ->
                    val record = uow.record as ScheduledEvent
                    val detailJson = record.detail.toJsonElement().toString()
                    val eventObj = eventCodec.decode(detailJson).let {
                        if (it.id == null) it.copyEvent(id = record.id) else it
                    }
                    uow.copy(event = eventObj)
                }
        }
    }

    fun fromScheduledEvent(event: ScheduledEvent): Flow<UnitOfWork> {
        // ScheduledEvent is already the event itself in this case
        val eventJson = event.toJsonElement().toString()
        val eventObj = JsonEventCodec.decode(eventJson)
        val timestamp = event.time?.millis ?: System.currentTimeMillis()
        return flowOf(
            UnitOfWork(record = event, event = eventObj).withMetrics(
                PipelineMetrics(
                    timer = Timer(
                        start = timestamp,
                        last = timestamp
                    )
                )
            )
        )
    }
}
