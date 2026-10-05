package io.kopipes.aws.connectors

import aws.sdk.kotlin.services.eventbridge.model.PutEventsRequest
import aws.sdk.kotlin.services.eventbridge.model.PutEventsRequestEntry
import aws.sdk.kotlin.services.eventbridge.model.PutEventsResponse
import aws.sdk.kotlin.services.eventbridge.model.PutEventsResultEntry
import io.kopipes.core.resilience.RetryStrategy

class EventBridgeRetryStrategy : RetryStrategy<PutEventsRequest, PutEventsResponse, ConnectorResponse> {

    override fun shouldRetry(response: PutEventsResponse): Boolean {
        return (response.failedEntryCount ?: 0) > 0
    }

    override fun nextRequest(
        originalRequest: PutEventsRequest,
        response: PutEventsResponse
    ): PutEventsRequest {
        val failedEntries = getFailedEntries(originalRequest.entries ?: emptyList(), response)
        return originalRequest.copy {
            entries = failedEntries
        }
    }

    override fun combineAttempts(
        attempts: List<PutEventsResponse>,
        finalResponse: PutEventsResponse
    ): ConnectorResponse {
        val totalAttempts = attempts + finalResponse
        val successfulEntries = totalAttempts.flatMap { attempt ->
            attempt.entries?.filter { it.errorCode == null } ?: emptyList()
        }
        val finalFailedCount = totalAttempts.lastOrNull()?.failedEntryCount ?: 0

        return ConnectorResponse(
            entries = successfulEntries,
            failedEntryCount = finalFailedCount,
            attempts = totalAttempts
        )
    }

    private fun getFailedEntries(
        originalEntries: List<PutEventsRequestEntry>,
        response: PutEventsResponse
    ): List<PutEventsRequestEntry> {
        val entries = response.entries ?: return emptyList()
        return originalEntries.filterIndexed { index, _ ->
            val entryResult = entries.getOrNull(index)
            isFailed(entryResult)
        }
    }

    private fun isFailed(entry: PutEventsResultEntry?): Boolean {
        return entry?.errorCode != null
    }
}
