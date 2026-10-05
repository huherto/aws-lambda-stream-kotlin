package io.kopipes.aws.from

import com.amazonaws.services.lambda.runtime.events.ScheduledEvent
import io.kopipes.core.*
import io.kopipes.core.faults.FaultManager
import io.kopipes.core.metrics.PipelineMetrics
import io.kopipes.core.metrics.Timer
import io.kopipes.core.metrics.withMetrics
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flowOf

class EventBridgeAdapter(
    private val faultManager: FaultManager = GlobalRegistry.faultManager(),
    private val toEvent: (Map<String, Any>) -> Event
) {

    // Use this when it is an event sent through EventBridge from another service
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

                    val eventObj = toEvent(record.detail).let {
                        if (it.id == null) it.copyEvent(id = record.id) else it
                    }

                    uow.copy(event = eventObj)
                }
        }
    }

    // Use this when it is a timer firing
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
