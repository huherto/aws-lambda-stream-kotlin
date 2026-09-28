package io.kopipes.aws.sinks

import io.kopipes.aws.UnitOfWork
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.onEach

/**
 * Interface for publishing units of work / events.
 */
interface EventPublisher {
    fun publish(flow: Flow<UnitOfWork>): Flow<UnitOfWork>
}

/**
 * In-memory test implementation of [EventPublisher].
 */
class EventPublisherInMemory : EventPublisher {
    private val uows = mutableListOf<UnitOfWork>()

    override fun publish(flow: Flow<UnitOfWork>): Flow<UnitOfWork> {
        return flow.onEach { uows.add(it) }
    }

    fun events() = uows.map { it.event }.toList()
    fun faults() = uows.map { it.fault }.toList()
    fun uows() = uows.toList()
}
