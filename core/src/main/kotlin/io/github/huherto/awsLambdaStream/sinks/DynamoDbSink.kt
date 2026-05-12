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

/**
 * Sink responsible for applying DynamoDB write operations produced by a stream of [UnitOfWork] items.
 *
 * Each [UnitOfWork] may contain a DynamoDB request to execute. If the expected request is not present,
 * the item is passed through unchanged. When a request is present, this sink delegates execution to the
 * configured [DynamoDbConnector] and returns a copy of the [UnitOfWork] containing the DynamoDB response.
 *
 * Operations are executed with bounded concurrency. The default parallelism is read from [EnvironmentConfig],
 * falling back to `4` when no value is configured.
 */
class DynamoDbSink(
    private val envConfig: EnvironmentConfig,
    private val connector: DynamoDbConnector,
    private val parallel: Int = envConfig.parallel() ?: 4,
) {
    /**
     * Executes DynamoDB update requests for each [UnitOfWork] in [source].
     *
     * If a unit of work does not contain an update request, it is emitted unchanged.
     * If the update succeeds, the returned unit of work contains the update response.
     * If the update fails, the failure is delegated to [rejectWithFault].
     *
     * @param source flow of units of work to process.
     * @return a flow containing the processed units of work.
     */
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

    /**
     * Executes DynamoDB put requests for each [UnitOfWork] in [source].
     *
     * If a unit of work does not contain a put request, it is emitted unchanged.
     * If the put succeeds, the returned unit of work contains the put response.
     * If the put fails, the failure is delegated to [rejectWithFault].
     *
     * @param source flow of units of work to process.
     * @return a flow containing the processed units of work.
     */
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


/**
 * Applies [transform] to values from this flow using bounded parallelism.
 *
 * At most [parallelism] transformations are active at the same time. Results are emitted as soon as
 * individual transformations complete, so output ordering is not guaranteed to match input ordering.
 *
 * @param parallelism maximum number of concurrently running transformations.
 * @param transform suspending transformation to apply to each value.
 * @return a flow containing transformed values.
 */
internal fun <T, R> Flow<T>.mapParallel(
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

internal suspend fun rejectWithFault(
    uow: UnitOfWork,
    error: Throwable,
): UnitOfWork {
    // Equivalent placeholder for `rejectWithFault(uow)` from TypeScript.
    // Replace with your project’s FaultManager/fault representation.
    throw error
}