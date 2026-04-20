package io.github.huherto.awsLambdaStream.filters

import io.github.huherto.awsLambdaStream.FaultManager
import io.github.huherto.awsLambdaStream.UnitOfWork
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.filter

fun Flow<UnitOfWork>.filterEvents(
    faultManager: FaultManager,
    filter: EventFilter
): Flow<UnitOfWork> = filter { uow ->
    with(faultManager) {
        faulty(uow) {
            val event = uow.event
            event != null && filter.matches(event)
        } == true
    }
}
