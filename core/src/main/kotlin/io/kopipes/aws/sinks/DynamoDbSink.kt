package io.kopipes.aws.sinks

import io.kopipes.aws.GlobalRegistry.envConfig
import io.kopipes.aws.UnitOfWork
import io.kopipes.aws.connectors.DynamoDbConnector
import io.kopipes.aws.extensions.putRequest
import io.kopipes.aws.extensions.updateRequest
import io.kopipes.aws.extensions.withPutResponse
import io.kopipes.aws.extensions.withUpdateResponse
import io.kopipes.aws.faults.FaultManager
import io.kopipes.aws.metrics.withStepMetrics
import io.kopipes.aws.utils.mapParallel
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

