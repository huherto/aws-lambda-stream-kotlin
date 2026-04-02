package io.github.huherto.awsLambdaStream.connectors

import aws.sdk.kotlin.runtime.auth.credentials.EnvironmentCredentialsProvider
import aws.sdk.kotlin.services.eventbridge.EventBridgeClient
import aws.sdk.kotlin.services.eventbridge.model.PutEventsRequest
import aws.sdk.kotlin.services.eventbridge.model.PutEventsResponse
import aws.sdk.kotlin.services.eventbridge.model.PutEventsResultEntry
import aws.smithy.kotlin.runtime.net.url.Url
import io.github.huherto.awsLambdaStream.EnvironmentConfig
import kotlinx.coroutines.delay
import java.util.concurrent.ConcurrentHashMap
import kotlin.time.Duration

// Configuration Models
data class RetryConfig(
    val maxRetries: Int = 3,
    val retryWait: Long = 1000L
)

data class ConnectorOptions(
    val metrics: Metrics? = null
)

// Defining an interface for the dynamically-called metrics capture
interface Metrics {
    fun capture(
        client: EventBridgeClient,
        request: PutEventsRequest,
        service: String,
        opt: ConnectorOptions,
        ctx: Any?
    )
}

// Wrapper to hold the accumulated result and the history of attempts
data class ConnectorResponse(
    val entries: List<PutEventsResultEntry>,
    val failedEntryCount: Int,
    val attempts: List<PutEventsResponse>
)

interface EventBridgeClientFactory {
    fun createClient(pipelineId: String): EventBridgeClient
}

class EventBridgeClientFactoryImpl(private val envConfig: EnvironmentConfig) : EventBridgeClientFactory {
    override fun createClient(pipelineId: String): EventBridgeClient {
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
    private val debug: (String, Any?) -> Unit = { msg, arg -> println(msg.replace("%j", arg.toString())) }
) {
    private val client: EventBridgeClient = getClient(pipelineId, clientFactory)

    private val logger = mu.KotlinLogging.logger {}

    companion object {
        private val clients = ConcurrentHashMap<String, EventBridgeClient>()

        fun getClient(pipelineId: String, clientFactory: EventBridgeClientFactory): EventBridgeClient {
            return clients.computeIfAbsent(pipelineId ) {
                clientFactory.createClient(pipelineId)
            }
        }

        // Useful for testing.
        internal fun clearClients() {
            clients.clear()
        }
    }

    suspend fun putEvents(params: PutEventsRequest, ctx: Any? = null): ConnectorResponse {
        return putEventsInternal(params, emptyList(), ctx)
    }

    internal suspend fun putEventsInternal(
        params: PutEventsRequest,
        attempts: List<PutEventsResponse>,
        ctx: Any?
    ): ConnectorResponse {
        assertMaxRetries(attempts.size, retryConfig.maxRetries)

        if (attempts.isNotEmpty()) {
            val delayMs = getDelay(retryConfig.retryWait, attempts.size)
            delay(delayMs)
        }

        val resp = sendCommand(params, ctx)

        return if (resp.failedEntryCount > 0) {
            putEventsInternal(unprocessed(params, resp), attempts + resp, ctx)
        } else {
            accumulate(attempts, resp)
        }
    }

    internal suspend fun sendCommand(request: PutEventsRequest, ctx: Any?): PutEventsResponse {
        opt.metrics?.capture(client, request, "eventbridge", opt, ctx)
        
        return try {
            val response = client.putEvents(request)
            logger.debug{"Success response:$response"}
            response
        } catch (e: Exception) {
            logger.warn{"Error sending command: ${e.message}"}
            throw e
        }
    }

    internal fun unprocessed(params: PutEventsRequest, resp: PutEventsResponse): PutEventsRequest {
        val failedEntries = params.entries?.filterIndexed { index, _ ->
            resp.entries?.get(index)?.errorCode != null
        }
        
        // AWS SDK for Kotlin provides a handy `.copy {}` builder for immutable updates
        return params.copy {
            entries = failedEntries
        }
    }

    internal fun accumulate(attempts: List<PutEventsResponse>, finalResp: PutEventsResponse): ConnectorResponse {
        val allAttempts = attempts + finalResp
        
        val allSuccessfulEntries = allAttempts.flatMap { attempt ->
            attempt.entries?.filter { it.errorCode == null } ?: emptyList()
        }

        return ConnectorResponse(
            entries = allSuccessfulEntries,
            failedEntryCount = finalResp.failedEntryCount,
            attempts = allAttempts
        )
    }

    internal fun assertMaxRetries(attemptsCount: Int, maxRetries: Int) {
        if (attemptsCount > maxRetries) {
            throw IllegalStateException("Maximum retry attempts exceeded.")
        }
    }

    internal fun getDelay(baseDelay: Long, attempt: Int): Long {
        // Equivalent to custom backoff delay behavior
        return baseDelay * (1L shl (attempt - 1)).coerceAtLeast(1L)
    }
}