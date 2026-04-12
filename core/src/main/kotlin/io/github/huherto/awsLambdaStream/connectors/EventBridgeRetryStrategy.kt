package io.github.huherto.awsLambdaStream.connectors

import aws.sdk.kotlin.services.eventbridge.model.PutEventsRequest
import aws.sdk.kotlin.services.eventbridge.model.PutEventsResponse

class EventBridgeRetryStrategy : RetryStrategy<PutEventsRequest, PutEventsResponse, ConnectorResponse> {
    override fun shouldRetry(response: PutEventsResponse): Boolean {
        return response.failedEntryCount > 0
    }

    override fun nextRequest(
        originalRequest: PutEventsRequest,
        response: PutEventsResponse
    ): PutEventsRequest {
        val failedEntries = originalRequest.entries?.filterIndexed { index, _ ->
            response.entries?.get(index)?.errorCode != null
        }

        return originalRequest.copy {
            entries = failedEntries
        }
    }

    override fun combineAttempts(
        attempts: List<PutEventsResponse>,
        finalResponse: PutEventsResponse
    ): ConnectorResponse {
        val allAttempts = attempts + finalResponse

        val allSuccessfulEntries = allAttempts.flatMap { attempt ->
            attempt.entries?.filter { it.errorCode == null } ?: emptyList()
        }

        return ConnectorResponse(
            entries = allSuccessfulEntries,
            failedEntryCount = finalResponse.failedEntryCount,
            attempts = allAttempts
        )
    }
}