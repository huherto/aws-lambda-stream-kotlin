package io.github.huherto.awsLambdaStream.utils

import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.buffer
import kotlinx.coroutines.flow.channelFlow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import kotlin.time.Duration.Companion.milliseconds

fun <T, R> Flow<T>.mapParallel(
    parallelism: Int,
    transform: suspend (T) -> R?,
): Flow<R> = channelFlow {
    val safeParallelism = parallelism.coerceAtLeast(1)
    val semaphore = Semaphore(safeParallelism)

    collect { value ->
        launch {
            semaphore.withPermit {
                val element = transform(value)
                if (element != null) {
                    send(element)
                }
            }
        }
    }
}.buffer(parallelism.coerceAtLeast(1))

fun <T> Flow<T>.rateLimit(
    rate: Int,
    windowMillis: Long,
): Flow<T> = flow {
    val safeRate = rate.coerceAtLeast(1)
    val safeWindowMillis = windowMillis.coerceAtLeast(0)
    var emittedInWindow = 0

    collect { value ->
        if (emittedInWindow == safeRate) {
            delay(safeWindowMillis.milliseconds)
            emittedInWindow = 0
        }

        emit(value)
        emittedInWindow += 1
    }
}
