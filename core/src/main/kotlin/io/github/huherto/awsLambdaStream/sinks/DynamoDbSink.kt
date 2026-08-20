package io.github.huherto.awsLambdaStream.sinks

import io.github.huherto.awsLambdaStream.EnvironmentConfig
import io.github.huherto.awsLambdaStream.FaultManager
import io.github.huherto.awsLambdaStream.GlobalRegistry.envConfig
import io.github.huherto.awsLambdaStream.UnitOfWork
import io.github.huherto.awsLambdaStream.connectors.DynamoDbConnector
import io.github.huherto.awsLambdaStream.extensions.putRequest
import io.github.huherto.awsLambdaStream.extensions.updateRequest
import io.github.huherto.awsLambdaStream.extensions.withPutResponse
import io.github.huherto.awsLambdaStream.extensions.withUpdateResponse
import io.github.huherto.awsLambdaStream.metrics.withStepMetrics
import io.github.huherto.awsLambdaStream.utils.mapParallel
import kotlinx.coroutines.flow.Flow

/**
 * Sink responsible for applying DynamoDB write operations produced by a stream of [UnitOfWork] items.
 *
 * Each [UnitOfWork] may contain a DynamoDB request to execute. If the expected request is not present,
 * the item is passed through unchanged. When a request is present, this sink delegates execution to the
 * configured [DynamoDbConnector] and returns a copy of the [UnitOfWork] containing the DynamoDB response.
 *
 * Operations are executed with bounded concurrency. The default parallelism is read from [EnvironmentConfig],
 * falling back to `4` when no value is configured.
 *
 * @param connector Connector used to execute DynamoDB requests.
 * @param parallel Bounded concurrency for DynamoDB operations.
 */
class DynamoDbSink(
    private val connector: DynamoDbConnector,
    private val parallel: Int = envConfig().parallel() ?: 4,
) {

    fun getConnector()  : DynamoDbConnector {
        return connector
    }

    /**
     * Executes DynamoDB update requests for each [UnitOfWork] in [source].
     *
     * If a unit of work does not contain an update request, it is emitted unchanged.
     * If the update succeeds, the returned unit of work contains the update response.
     * If the update fails, the failure is handled by the [FaultManager].
     *
     * @param source flow of units of work to process.
     * @return a flow containing the processed units of work.
     */
    fun update(fm: FaultManager, source: Flow<UnitOfWork>): Flow<UnitOfWork> =
        source
            .mapParallel(parallel) { uow ->
                val request = uow.updateRequest ?: return@mapParallel uow
                fm.faulty(uow) {
                    it.withStepMetrics("update") { uowWithMetrics ->
                        val updateResponse = getConnector().update(uowWithMetrics.updateRequest!!, uowWithMetrics)
                        uowWithMetrics.withUpdateResponse(updateResponse)
                    }
                }
            }

    /**
     * Executes DynamoDB put requests for each [UnitOfWork] in [source].
     *
     * If a unit of work does not contain a put request, it is emitted unchanged.
     * If the put succeeds, the returned unit of work contains the put response.
     * If the update fails, the failure is handled by the [FaultManager].
     *
     * @param source flow of units of work to process.
     * @return a flow containing the processed units of work.
     */
    fun put(fm: FaultManager, source: Flow<UnitOfWork>): Flow<UnitOfWork> =
        source
            .mapParallel(parallel) { uow ->
                val request = uow.putRequest ?: return@mapParallel uow
                fm.faulty(uow) {
                    it.withStepMetrics("put") { uowWithMetrics ->
                        val putResponse = getConnector().put(uowWithMetrics.putRequest!!, uowWithMetrics)
                        uowWithMetrics.withPutResponse(putResponse)
                    }
                }
            }
}

