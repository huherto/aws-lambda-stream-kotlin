package io.github.huherto.awsLambdaStream.connectors

import kotlinx.coroutines.delay
import kotlin.time.Duration.Companion.milliseconds

data class RetryConfig(
    val maxRetries: Int = 3,
    val retryWait: Long = 1000L
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
                delay(delayDuration.milliseconds)
            }

            val response = send(currentRequest)
            attempts += response

            if (!strategy.shouldRetry(response)) {
                return strategy.combineAttempts(attempts.dropLast(1), response)
            }

            currentRequest = strategy.nextRequest(currentRequest, response)
        }
    }

    private fun getDelay(baseDelay: Long, attempt: Int): Long {
        return baseDelay * (1L shl (attempt - 1)).coerceAtLeast(1L)
    }
}