package io.github.huherto.`aws-lambda-stream`

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.catch
import kotlinx.coroutines.flow.filter
import kotlin.reflect.KClass
import java.util.UUID

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

/*
 * Catch all the vents
 */
fun Flow<UnitOfWork>.catchFailures(): Flow<UnitOfWork> = catch { exception ->
    logError(exception)
    if (exception is FailureException) {
        val functionName = EnvironmentConfig().awsLambdaFunctionName()?: "undefined"
        val failureEvent = FailureEvent().apply {
            id = UUID.randomUUID().toString()
            partitionKey = UUID.randomUUID().toString()
            type = "FAILURE_EVENT"
            timestamp = System.currentTimeMillis()
            tags = mutableMapOf(
                "functionname" to functionName
            )
            failureException = exception
        }
    }
    else {
        // Not sure what to do here.
        throw exception
    }
}

fun logError(exception: Throwable) {
    println("Exception in lambda handler: $exception")
}

