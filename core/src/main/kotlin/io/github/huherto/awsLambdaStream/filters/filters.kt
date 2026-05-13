package io.github.huherto.awsLambdaStream.filters

import io.github.huherto.awsLambdaStream.EnvironmentConfig
import io.github.huherto.awsLambdaStream.FaultManager
import io.github.huherto.awsLambdaStream.UnitOfWork
import io.github.huherto.awsLambdaStream.from.RecordPair
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
            logger.debug{"Event filter matches: ${matches}, event: $eventType, pipelineId: $pipelineId"}
            matches
        } == true
    }
}

fun outSourceIsSelf(envConfig: EnvironmentConfig, uow: UnitOfWork) : Boolean {
    val source = uow.event?.tags?.get("source") ?: return true
    val project = envConfig.project() ?: envConfig.serverlessProject()
    return source != project
}

fun outLatched(uow: UnitOfWork): Boolean {
    val raw = uow.event?.raw as? RecordPair ?: return false

    val record = raw.new ?: raw.old
    return record?.let { !it.latched() } ?: false
}
