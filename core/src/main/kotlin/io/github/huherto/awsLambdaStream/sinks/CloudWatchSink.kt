package io.github.huherto.awsLambdaStream.sinks

import io.github.huherto.awsLambdaStream.EnvironmentConfig
import io.github.huherto.awsLambdaStream.FaultManager
import io.github.huherto.awsLambdaStream.UnitOfWork
import io.github.huherto.awsLambdaStream.connectors.CloudWatchClientFactory
import io.github.huherto.awsLambdaStream.connectors.CloudWatchConnector
import io.github.huherto.awsLambdaStream.connectors.DefaultCloudWatchClientFactory
import io.github.huherto.awsLambdaStream.extensions.copyCloudWatch
import io.github.huherto.awsLambdaStream.extensions.putMetricDataRequest
import io.github.huherto.awsLambdaStream.metrics.withStepMetrics
import io.github.huherto.awsLambdaStream.utils.mapParallel
import kotlinx.coroutines.flow.Flow
import mu.KotlinLogging

class CloudWatchSink(
    private val envConfig: EnvironmentConfig,
    private val clientFactory: CloudWatchClientFactory = DefaultCloudWatchClientFactory(envConfig),
    private val parallel: Int = envConfig.cloudWatchParallel() ?: envConfig.parallel() ?: 8,
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
                it.withStepMetrics("put-metrics", envConfig) { uowWithMetrics ->
                    val connector = CloudWatchConnector(
                        pipelineId = uowWithMetrics.pipeline?.id ?: "undefined",
                        envConfig = envConfig,
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