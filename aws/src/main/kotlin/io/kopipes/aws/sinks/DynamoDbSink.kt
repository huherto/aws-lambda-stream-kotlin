package io.kopipes.aws.sinks

import io.kopipes.aws.AwsKopipesContext
import io.kopipes.aws.connectors.DynamoDbConnector
import io.kopipes.aws.extensions.putRequest
import io.kopipes.aws.extensions.updateRequest
import io.kopipes.aws.extensions.withPutResponse
import io.kopipes.aws.extensions.withUpdateResponse
import io.kopipes.core.UnitOfWork
import io.kopipes.core.faults.FaultManager
import io.kopipes.core.metrics.withStepMetrics
import io.kopipes.core.utils.mapParallel
import kotlinx.coroutines.flow.Flow

/** Sink for applying DynamoDB write operations. */
class DynamoDbSink(
    private val connector: DynamoDbConnector,
    private val parallel: Int = DEFAULT_PARALLEL,
    private val stepMetrics: Boolean = false,
) {

    constructor(context: AwsKopipesContext) : this(
        connector = DynamoDbConnector(DynamoDbConnector.Options(context.dynamoDbClientFactory)),
        parallel = context.envConfig.parallel() ?: DEFAULT_PARALLEL,
        stepMetrics = context.stepMetrics,
    )

    companion object {
        const val DEFAULT_PARALLEL = 4
    }

    fun getConnector(): DynamoDbConnector {
        return connector
    }

    fun update(fm: FaultManager, source: Flow<UnitOfWork>): Flow<UnitOfWork> =
        source
            .mapParallel(parallel) { uow ->
                val request = uow.updateRequest ?: return@mapParallel uow
                fm.faulty(uow) { item ->
                    item.withStepMetrics("update", stepMetrics) { uowWithMetrics ->
                        val updateResponse = getConnector().update(uowWithMetrics.updateRequest!!, uowWithMetrics)
                        uowWithMetrics.withUpdateResponse(updateResponse)
                    }
                }
            }

    fun put(fm: FaultManager, source: Flow<UnitOfWork>): Flow<UnitOfWork> =
        source
            .mapParallel(parallel) { uow ->
                val request = uow.putRequest ?: return@mapParallel uow
                fm.faulty(uow) { item ->
                    item.withStepMetrics("put", stepMetrics) { uowWithMetrics ->
                        val putResponse = getConnector().put(uowWithMetrics.putRequest!!, uowWithMetrics)
                        uowWithMetrics.withPutResponse(putResponse)
                    }
                }
            }
}
