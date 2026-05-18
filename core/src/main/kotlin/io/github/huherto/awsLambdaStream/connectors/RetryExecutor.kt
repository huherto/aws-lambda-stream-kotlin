package io.github.huherto.awsLambdaStream.connectors

import kotlinx.coroutines.delay
import kotlin.random.Random
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

data class RetryConfig(
    val maxRetries: Int = 3,
    val retryWait: Duration = 1000.milliseconds
)

interface RetryStrategy<Request, Response, Result> {
    fun shouldRetry(response: Response): Boolean
    fun nextRequest(originalRequest: Request, response: Response): Request
    fun combineAttempts(attempts: List<Response>, finalResponse: Response): Result
}

class RetryExecutor<Request, Response, Result>(
    private val retryConfig: RetryConfig,
    private val strategy: RetryStrategy<Request, Response, Result>,
    private val send: suspend (Request) -> Response
) {
    suspend fun execute(request: Request): Result {
        var currentRequest = request
        val attempts = mutableListOf<Response>()

        while (true) {
            if (attempts.size > retryConfig.maxRetries) {
                throw IllegalStateException("Maximum retry attempts exceeded.")
            }

            if (attempts.isNotEmpty()) {
                val delayDuration = getDelay(retryConfig.retryWait, attempts.size)
                delay(delayDuration)
            }

            val response = send(currentRequest)
            attempts += response

            if (!strategy.shouldRetry(response)) {
                return strategy.combineAttempts(attempts.dropLast(1), response)
            }

            currentRequest = strategy.nextRequest(currentRequest, response)
        }
    }

    private fun getDelay(baseDelay: Duration, attempt: Int): Duration {
        val shift = (attempt - 1).coerceAtMost(30)
        val multiplier = 1 shl shift
        val jitterFactor = Random.nextDouble(0.5, 1.5)
        return baseDelay
            .times(multiplier)
            .times(jitterFactor)
            .coerceAtMost(60.seconds)
            .coerceAtLeast(250.milliseconds)
    }
}
