package io.github.huherto.awsLambdaStream.sinks

import io.github.huherto.awsLambdaStream.GlobalRegistry.envConfig
import io.github.huherto.awsLambdaStream.UnitOfWork
import io.github.huherto.awsLambdaStream.connectors.DynamoDbConnector
import io.github.huherto.awsLambdaStream.extensions.putRequest
import io.github.huherto.awsLambdaStream.extensions.updateRequest
import io.github.huherto.awsLambdaStream.extensions.withPutResponse
import io.github.huherto.awsLambdaStream.extensions.withUpdateResponse
import io.github.huherto.awsLambdaStream.faults.FaultManager
import io.github.huherto.awsLambdaStream.metrics.withStepMetrics
import io.github.huherto.awsLambdaStream.utils.mapParallel
import kotlinx.coroutines.flow.Flow

/** Sink for applying DynamoDB write operations. */
class DynamoDbSink(
    private val connector: DynamoDbConnector,
    private val parallel: Int = envConfig().parallel() ?: 4,
) {

    fun getConnector()  : DynamoDbConnector {
        return connector
    }

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

