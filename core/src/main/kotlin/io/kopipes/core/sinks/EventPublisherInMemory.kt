package io.kopipes.core.sinks

import io.kopipes.core.UnitOfWork
import io.kopipes.core.faults.FaultEvent
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.onEach

class EventPublisherInMemory : EventPublisher {
    private val uows = mutableListOf<UnitOfWork>()

    override fun publish(flow: Flow<UnitOfWork>): Flow<UnitOfWork> {
        return flow.onEach { uow ->
            uows.add(uow)
        }
    }

    fun getUows(): List<UnitOfWork> = uows.toList()

    fun uows(): List<UnitOfWork> = getUows()

    fun faults(): List<FaultEvent> = uows.mapNotNull { it.fault }

    fun reset() {
        uows.clear()
    }
}
