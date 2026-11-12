package io.kopipes.aws.sinks

import io.kopipes.aws.AwsKopipesContext
import io.kopipes.aws.connectors.CloudWatchClientFactory
import io.kopipes.aws.connectors.CloudWatchConnector
import io.kopipes.aws.extensions.copyCloudWatch
import io.kopipes.aws.extensions.putMetricDataRequest
import io.kopipes.core.UnitOfWork
import io.kopipes.core.faults.FaultManager
import io.kopipes.core.metrics.withStepMetrics
import io.kopipes.core.utils.mapParallel
import kotlinx.coroutines.flow.Flow
import mu.KotlinLogging

class CloudWatchSink(
    private val clientFactory: CloudWatchClientFactory,
    private val parallel: Int = 8,
    private val stepMetrics: Boolean = false,
) {

    constructor(context: AwsKopipesContext) : this(
        clientFactory = context.cloudWatchClientFactory,
        parallel = context.envConfig.cloudWatchParallel() ?: context.envConfig.parallel() ?: 8,
        stepMetrics = context.stepMetrics,
    )

    private val logger = KotlinLogging.logger {}

    fun putMetrics(fm: FaultManager, source: Flow<UnitOfWork>): Flow<UnitOfWork> =
        source.mapParallel(parallel) { uow ->
            val request = uow.putMetricDataRequest
            if (request == null) {
                logger.debug { "No PutMetricDataRequest found in UnitOfWork, skipping" }
                return@mapParallel uow
            }
            logger.debug { "Sending metrics to CloudWatch: $request" }
            fm.faulty(uow) { item ->
                item.withStepMetrics("put-metrics", stepMetrics) { uowWithMetrics ->
                    val connector = CloudWatchConnector(
                        pipelineId = uowWithMetrics.pipeline?.id ?: "undefined",
                        clientFactory = clientFactory
                    )
                    val response = connector.putMetricData(uowWithMetrics.putMetricDataRequest!!)
                    uowWithMetrics.copyCloudWatch {
                        copy(putMetricDataResponse = response)
                    }
                }
            }
        }
}
