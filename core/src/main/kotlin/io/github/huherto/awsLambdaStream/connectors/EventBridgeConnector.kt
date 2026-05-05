package io.github.huherto.awsLambdaStream.connectors

import aws.sdk.kotlin.runtime.auth.credentials.EnvironmentCredentialsProvider
import aws.sdk.kotlin.services.eventbridge.EventBridgeClient
import aws.sdk.kotlin.services.eventbridge.model.PutEventsRequest
import aws.sdk.kotlin.services.eventbridge.model.PutEventsResponse
import aws.smithy.kotlin.runtime.net.url.Url
import io.github.huherto.awsLambdaStream.EnvironmentConfig
import java.util.concurrent.ConcurrentHashMap
import kotlin.time.Duration

data class ConnectorOptions(
    val metrics: Metrics? = null
)

interface Metrics {
    fun capture(
        client: EventBridgeClient,
        request: PutEventsRequest,
        service: String,
        opt: ConnectorOptions,
        ctx: Any?
    )
}

data class ConnectorResponse(
    val entries: List<aws.sdk.kotlin.services.eventbridge.model.PutEventsResultEntry>,
    val failedEntryCount: Int,
    val attempts: List<PutEventsResponse>
)

interface EventBridgeClientFactory {
    fun createClient(): EventBridgeClient
}

class DefaultEventBridgeClientFactory(private val envConfig: EnvironmentConfig) : EventBridgeClientFactory {
    override fun createClient(): EventBridgeClient {
        val endpointUrl = envConfig.endPointUrl()?.ifEmpty { null }
        val region = envConfig.awsRegion()
        return EventBridgeClient {
            this.region = region
            this.credentialsProvider = EnvironmentCredentialsProvider()
            endpointUrl?.let { this.endpointUrl = Url.parse(it) }
        }
    }
}

class EventBridgeConnector(
    pipelineId: String,
    private val envConfig: EnvironmentConfig,
    timeout: Duration,
    private val retryConfig: RetryConfig,
    private val opt: ConnectorOptions = ConnectorOptions(),
    private val clientFactory: EventBridgeClientFactory,
) {
    private val client: EventBridgeClient = getClient(pipelineId, clientFactory)
    private val logger = mu.KotlinLogging.logger {}
    private val retryExecutor = RetryExecutor(
        retryConfig = retryConfig,
        strategy = EventBridgeRetryStrategy(),
        send = { request -> sendCommand(request, null) }
    )

    companion object {
        private val clients = ConcurrentHashMap<String, EventBridgeClient>()

        fun getClient(pipelineId: String, clientFactory: EventBridgeClientFactory): EventBridgeClient {
            return clients.computeIfAbsent(pipelineId) {
                clientFactory.createClient()
            }
        }

        internal fun clearClients() {
            clients.clear()
        }
    }

    suspend fun putEvents(params: PutEventsRequest, ctx: Any? = null): ConnectorResponse {
        return retryExecutor.execute(params)
    }

    internal suspend fun sendCommand(request: PutEventsRequest, ctx: Any?): PutEventsResponse {
        opt.metrics?.capture(client, request, "eventbridge", opt, ctx)

        return try {
            val response = client.putEvents(request)
            logger.debug { "Success response:$response" }
            response
        } catch (e: Exception) {
            logger.warn { "Error sending command: ${e.message}" }
            throw e
        }
    }
}