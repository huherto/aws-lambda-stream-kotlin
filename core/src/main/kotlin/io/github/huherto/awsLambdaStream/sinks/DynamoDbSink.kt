package io.github.huherto.awsLambdaStream.sinks

import io.github.huherto.awsLambdaStream.EnvironmentConfig
import io.github.huherto.awsLambdaStream.UnitOfWork
import io.github.huherto.awsLambdaStream.connectors.DynamoDbConnector
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.buffer
import kotlinx.coroutines.flow.channelFlow
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit

class DynamoDbSink(
    private val envConfig: EnvironmentConfig,
    private val connector: DynamoDbConnector,
    private val parallel: Int = envConfig.parallel() ?: 4,
) {
    fun update(source: Flow<UnitOfWork>): Flow<UnitOfWork> =
        source
            .mapParallel(parallel) { uow ->
                val request = uow.updateRequest ?: return@mapParallel uow

                try {
                    val updateResponse =  connector.update(request, uow)

                    uow.copy(updateResponse = updateResponse)
                } catch (error: Throwable) {
                    rejectWithFault(uow, error)
                }
            }

    fun put(source: Flow<UnitOfWork>): Flow<UnitOfWork> =
        source
            .mapParallel(parallel) { uow ->
                val request = uow.putRequest ?: return@mapParallel uow

                try {
                    val putResponse = connector.put(request, uow)

                    uow.copy(putResponse = putResponse)
                } catch (error: Throwable) {
                    rejectWithFault(uow, error)
                }
            }
}


private fun <T, R> Flow<T>.mapParallel(
    parallelism: Int,
    transform: suspend (T) -> R,
): Flow<R> = channelFlow {
    val semaphore = Semaphore(parallelism)

    collect { value ->
        launch {
            semaphore.withPermit {
                send(transform(value))
            }
        }
    }
}.buffer(parallelism)

private suspend fun rejectWithFault(
    uow: UnitOfWork,
    error: Throwable,
): UnitOfWork {
    // Equivalent placeholder for `rejectWithFault(uow)` from TypeScript.
    // Replace with your project’s FaultManager/fault representation.
    throw error
}

private object Undefined