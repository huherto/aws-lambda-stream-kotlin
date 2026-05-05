package io.github.huherto.awsLambdaStream.connectors

import aws.sdk.kotlin.services.dynamodb.model.AttributeValue
import aws.sdk.kotlin.services.dynamodb.model.BatchGetItemRequest
import aws.sdk.kotlin.services.dynamodb.model.BatchGetItemResponse

class DynamoDbBatchGetRetryStrategy :
    RetryStrategy<BatchGetItemRequest, BatchGetItemResponse, BatchGetItemResponse> {

    override fun shouldRetry(response: BatchGetItemResponse): Boolean {
        return response.unprocessedKeys.orEmpty().isNotEmpty()
    }

    override fun nextRequest(
        originalRequest: BatchGetItemRequest,
        response: BatchGetItemResponse,
    ): BatchGetItemRequest {
        return originalRequest.copy {
            requestItems = response.unprocessedKeys
        }
    }

    override fun combineAttempts(
        attempts: List<BatchGetItemResponse>,
        finalResponse: BatchGetItemResponse,
    ): BatchGetItemResponse {
        return accumulate(attempts, finalResponse)
    }

    private fun accumulate(
        attempts: List<BatchGetItemResponse>,
        response: BatchGetItemResponse,
    ): BatchGetItemResponse {
        val allResponses = linkedMapOf<String, MutableList<Map<String, AttributeValue>>>()

        fun merge(resp: BatchGetItemResponse) {
            resp.responses.orEmpty().forEach { (tableName, items) ->
                allResponses
                    .getOrPut(tableName) { mutableListOf() }
                    .addAll(items)
            }
        }

        attempts.forEach(::merge)
        merge(response)

        return response.copy {
            responses = allResponses
        }
    }
}