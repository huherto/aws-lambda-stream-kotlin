package io.github.huherto.awsLambdaStream.from

import io.github.huherto.awsLambdaStream.BaseEvent
import io.github.huherto.awsLambdaStream.EventReference
import io.github.huherto.awsLambdaStream.RawRecord
import io.github.huherto.awsLambdaStream.UnitOfWork
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flowOf
import kotlinx.datetime.Clock
import java.util.*

class CwAdapter {
    fun fromAlarm(event: Any): Flow<UnitOfWork> {
        val eventObj = AlarmEvent(
            id = UUID.randomUUID().toString(),
            record = event
        )
        return flowOf(UnitOfWork(record = event, event = eventObj))
    }
}

data class AlarmEvent(
    override val id: String?,
    val record: Any,
    override val timestamp: Long? = Clock.System.now().toEpochMilliseconds(),
    override val partitionKey: String? = null,
    override val tags: Map<String, String>? = null,
    override val raw: RawRecord? = null,
    override val eem: Any? = null,
    override val triggers: List<EventReference>? = null
) : BaseEvent() {
    override fun eventType(): String = "aws-cloudwatch-alarm"
}
