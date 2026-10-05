package io.kopipes.core.filters

import io.kopipes.core.UnitOfWork
import io.kopipes.core.faults.FaultManager
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.filter
import mu.KotlinLogging

fun Flow<UnitOfWork>.filterEvents(
    faultManager: FaultManager,
    filter: EventFilter
): Flow<UnitOfWork> = filter { uow ->
    val logger = KotlinLogging.logger { }
    with(faultManager) {
        faulty(uow) {
            val event = uow.event
            val matches = event != null && filter.matches(event)
            val eventType = event?.eventType() ?: "unknown"
            val pipelineId = uow.pipeline?.id ?: "unknown"
            logger.debug { "Event filter matches: $matches, event: $eventType, pipelineId: $pipelineId" }
            matches
        } == true
    }
}
