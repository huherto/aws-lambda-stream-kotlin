package io.kopipes.aws.sinks

import io.kopipes.aws.GlobalRegistry.envConfig
import io.kopipes.aws.UnitOfWork
import io.kopipes.aws.connectors.CloudWatchClientFactory
import io.kopipes.aws.connectors.CloudWatchConnector
import io.kopipes.aws.connectors.DefaultCloudWatchClientFactory
import io.kopipes.aws.extensions.copyCloudWatch
import io.kopipes.aws.extensions.putMetricDataRequest
import io.kopipes.aws.faults.FaultManager
import io.kopipes.aws.metrics.withStepMetrics
import io.kopipes.aws.utils.mapParallel
import kotlinx.coroutines.flow.Flow
import mu.KotlinLogging

class CloudWatchSink(
    private val clientFactory: CloudWatchClientFactory = DefaultCloudWatchClientFactory(),
    private val parallel: Int = envConfig().cloudWatchParallel() ?: envConfig().parallel() ?: 8,
) {
    private val logger = KotlinLogging.logger {}

    fun putMetrics(fm: FaultManager, source: Flow<UnitOfWork>): Flow<UnitOfWork> =
        source.mapParallel(parallel) { uow ->
            val request = uow.putMetricDataRequest
            if (request == null) {
                logger.debug { "No PutMetricDataRequest found in UnitOfWork, skipping" }
                return@mapParallel uow
            }
            logger.debug { "Sending metrics to CloudWatch: $request" }
            fm.faulty(uow) {
                it.withStepMetrics("put-metrics") { uowWithMetrics ->
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