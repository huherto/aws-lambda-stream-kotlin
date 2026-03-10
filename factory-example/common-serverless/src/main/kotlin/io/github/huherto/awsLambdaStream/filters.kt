package io.github.huherto.awsLambdaStream

import faulty
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.filter
import kotlin.reflect.KClass


fun Flow<UnitOfWork>.filterEventTypes(vararg klassList: KClass<out Event>): Flow<UnitOfWork> = filter {
    faulty(it) {
        val currentEvent = it.event
        currentEvent != null && klassList.any { clazz -> clazz.isInstance(currentEvent) }
    } == true
}

