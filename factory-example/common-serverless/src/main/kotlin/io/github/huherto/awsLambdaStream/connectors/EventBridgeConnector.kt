import aws.sdk.kotlin.services.eventbridge.EventBridgeClient
import aws.sdk.kotlin.services.eventbridge.model.PutEventsRequest
import aws.sdk.kotlin.services.eventbridge.model.PutEventsResponse
import aws.sdk.kotlin.services.eventbridge.model.PutEventsResultEntry
import kotlinx.coroutines.delay
import java.util.concurrent.ConcurrentHashMap
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds

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

class EventBridgeConnector(
    private val pipelineId: String,
    private val timeout: Duration = getTimeoutFromEnv(),
    private val retryConfig: RetryConfig = RetryConfig(),
    private val opt: ConnectorOptions = ConnectorOptions(),
    private val debug: (String, Any?) -> Unit = { msg, arg -> println(msg.replace("%j", arg.toString())) }
) {
    private val client: EventBridgeClient = getClient(pipelineId, timeout)

    companion object {
        private val clients = ConcurrentHashMap<String, EventBridgeClient>()

        private fun getTimeoutFromEnv(): Duration {
            val timeoutMs = System.getenv("BUS_TIMEOUT")?.toLongOrNull()
                ?: System.getenv("TIMEOUT")?.toLongOrNull()
                ?: 1000L
            return timeoutMs.milliseconds
        }

        fun getClient(pipelineId: String, timeout: Duration): EventBridgeClient {
            return clients.computeIfAbsent(pipelineId) {
                EventBridgeClient {
                    // SDK-level retry strategies and Http Engine timeouts can be configured here
                }
            }
        }
    }

    suspend fun putEvents(params: PutEventsRequest, ctx: Any? = null): ConnectorResponse {
        return putEventsInternal(params, emptyList(), ctx)
    }

    private suspend fun putEventsInternal(
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

    private suspend fun sendCommand(request: PutEventsRequest, ctx: Any?): PutEventsResponse {
        opt.metrics?.capture(client, request, "eventbridge", opt, ctx)
        
        return try {
            val response = client.putEvents(request)
            debug("Success response:", response)
            response
        } catch (e: Exception) {
            debug("Error sending command:", e.message)
            throw e
        }
    }

    private fun unprocessed(params: PutEventsRequest, resp: PutEventsResponse): PutEventsRequest {
        val failedEntries = params.entries?.filterIndexed { index, _ ->
            resp.entries?.get(index)?.errorCode != null
        }
        
        // AWS SDK for Kotlin provides a handy `.copy {}` builder for immutable updates
        return params.copy {
            entries = failedEntries
        }
    }

    private fun accumulate(attempts: List<PutEventsResponse>, finalResp: PutEventsResponse): ConnectorResponse {
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

    private fun assertMaxRetries(attemptsCount: Int, maxRetries: Int) {
        if (attemptsCount > maxRetries) {
            throw IllegalStateException("Maximum retry attempts exceeded.")
        }
    }

    private fun getDelay(baseDelay: Long, attempt: Int): Long {
        // Equivalent to custom backoff delay behavior
        return baseDelay * (1L shl (attempt - 1)).coerceAtLeast(1L)
    }
}