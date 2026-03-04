package io.github.huherto.awsLambdaStream

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.filter
import kotlin.reflect.KClass

inline fun <T, R> T.faulty(uom: UnitOfWork, block: T.() -> R): R {
    return try {
        block()
    } catch (e: Throwable) {
        val failureException = FailureException(uom, e)
        throw failureException
    }
}

fun Flow<UnitOfWork>.filterEventTypes(vararg klassList: KClass<out Event>): Flow<UnitOfWork> = filter {
    faulty(it) {
        val currentEvent = it.event
        currentEvent != null && klassList.any { clazz -> clazz.isInstance(currentEvent) }
    }
}

