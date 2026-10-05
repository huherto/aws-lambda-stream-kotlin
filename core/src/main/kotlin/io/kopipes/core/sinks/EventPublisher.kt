package io.kopipes.core.sinks

import io.kopipes.core.UnitOfWork
import kotlinx.coroutines.flow.Flow

interface EventPublisher {
    fun publish(flow: Flow<UnitOfWork>): Flow<UnitOfWork>
}
