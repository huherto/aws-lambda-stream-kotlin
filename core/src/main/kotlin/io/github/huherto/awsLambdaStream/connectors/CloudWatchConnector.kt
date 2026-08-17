package io.github.huherto.awsLambdaStream.connectors

import aws.sdk.kotlin.runtime.auth.credentials.EnvironmentCredentialsProvider
import aws.sdk.kotlin.services.cloudwatch.CloudWatchClient
import aws.sdk.kotlin.services.cloudwatch.model.PutMetricDataRequest
import aws.sdk.kotlin.services.cloudwatch.model.PutMetricDataResponse
import aws.smithy.kotlin.runtime.net.url.Url
import io.github.huherto.awsLambdaStream.GlobalRegistry.envConfig
import mu.KotlinLogging

interface CloudWatchClientFactory : ClientFactory<CloudWatchClient>

class DefaultCloudWatchClientFactory() : CloudWatchClientFactory, AbstractClientFactory<CloudWatchClient>() {
    override fun create(): CloudWatchClient {
        val endpointUrl = envConfig().endPointUrl()?.ifEmpty { null }
        val region = envConfig().awsRegion()
        return CloudWatchClient {
            this.region = region
            this.credentialsProvider = EnvironmentCredentialsProvider()
            endpointUrl?.let { this.endpointUrl = Url.parse(it) }
        }
    }
}

class CloudWatchConnector(
    pipelineId: String,
    clientFactory: CloudWatchClientFactory = DefaultCloudWatchClientFactory(),
) {
    private val client: CloudWatchClient = clientFactory.getClient(pipelineId)
    private val logger = KotlinLogging.logger {}

    suspend fun putMetricData(request: PutMetricDataRequest): PutMetricDataResponse {
        return try {
            val response = client.putMetricData(request)
            logger.debug { "Success response: $response" }
            response
        } catch (e: Exception) {
            logger.warn { "Error sending CloudWatch metric data: ${e.message}" }
            throw e
        }
    }
}
