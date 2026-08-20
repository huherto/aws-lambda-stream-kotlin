package io.github.huherto.awsLambdaStream.connectors

import aws.sdk.kotlin.runtime.auth.credentials.EnvironmentCredentialsProvider
import aws.sdk.kotlin.services.dynamodb.DynamoDbClient
import aws.sdk.kotlin.services.dynamodb.model.*
import aws.smithy.kotlin.runtime.net.url.Url
import io.github.huherto.awsLambdaStream.GlobalRegistry
import io.github.huherto.awsLambdaStream.GlobalRegistry.envConfig
import io.github.huherto.awsLambdaStream.UnitOfWork

interface DynamoDbClientFactory : ClientFactory<DynamoDbClient>

class DefaultDynamoDbClientFactory() : DynamoDbClientFactory, AbstractClientFactory<DynamoDbClient>() {
    override fun create(): DynamoDbClient {
        val endpointUrl = envConfig().endPointUrl()
        val region = envConfig().awsRegion()
        return DynamoDbClient {
            this.region = region
            this.credentialsProvider = EnvironmentCredentialsProvider()

            // If an endpoint URL is provided (like http://localhost:4566), use it
            endpointUrl?.let { this.endpointUrl = Url.parse(it) }

        }
    }
}

/**
 * Connector responsible for executing DynamoDB operations.
 *
 * This connector manages the interaction with the AWS SDK DynamoDB client, providing high-level
 * methods for common operations like queryAll, update, put, and batch-get. It supports automatic
 * pagination for queries, conditional failure handling, and retry strategies for batch operations.
 *
 * The connector is configured via [Options].
 *
 * @param options Configuration options for the connector.
 */
class DynamoDbConnector(private val options: Options) {

    data class Options(
        val debug: (Any?) -> Unit = {},
        val throwConditionFailure: Boolean = false,
        val dynamoDbClientFactory: DynamoDbClientFactory = GlobalRegistry.dynamoDbClientFactory(),
        val retryConfig: RetryConfig = RetryConfig(),
    )

    fun getClient(uow: UnitOfWork): DynamoDbClient {
        val pipelineId = uow.pipeline?.id ?: "unknown"
        return options.dynamoDbClientFactory.getClient(pipelineId)
    }

    suspend fun queryAll(
        queryRequest: QueryRequest,
        uow: UnitOfWork,
    ): QueryResponse {
        val client = getClient(uow)

        var cursor: Map<String, AttributeValue>? = queryRequest.exclusiveStartKey
        var itemsCount = 0
        val allItems = mutableListOf<Map<String, AttributeValue>>()
        var lastResponse: QueryResponse?

        do {
            val currentRequest = queryRequest.copy {
                exclusiveStartKey = cursor
            }

            val response = sendCommand {
                client.query(currentRequest)
            }

            val items = response.items ?: emptyList()
            itemsCount += items.size
            allItems += items
            lastResponse = response

            cursor =
                if (
                    response.lastEvaluatedKey?.isNotEmpty() == true &&
                    (currentRequest.limit == null || itemsCount < currentRequest.limit!!)
                ) {
                    response.lastEvaluatedKey
                } else {
                    null
                }
        } while (cursor != null)

        return lastResponse.copy {
            items = allItems
            lastEvaluatedKey = null
            count = allItems.size
        }
    }

    suspend fun update(
        updateRequest: UpdateItemRequest,
        uow: UnitOfWork,
    ): UpdateItemResponse? {
        return try {
            val client = getClient(uow)
            sendCommand() {
                client.updateItem(updateRequest)
            }
        } catch (error: ConditionalCheckFailedException) {
            if (options.throwConditionFailure) {
                throw error
            }
            null
        }
    }

    suspend fun put(
        putRequest: PutItemRequest,
        uow: UnitOfWork,
    ): PutItemResponse {

        val client = getClient(uow)
        return sendCommand() {
            client.putItem(putRequest)
        }
    }

    suspend fun batchGetItem(
        batchGetRequest: BatchGetItemRequest,
        uow: UnitOfWork,
    ): BatchGetItemResponse {
        val client = getClient(uow)

        return RetryExecutor(
            retryConfig = options.retryConfig,
            strategy = DynamoDbBatchGetRetryStrategy(),
            send = { request ->
                sendCommand {
                    client.batchGetItem(request)
                }
            },
        ).execute(batchGetRequest)
    }

    private suspend fun <T> sendCommand(
        block: suspend () -> T,
    ): T {

        return try {
            val response = block()
            options.debug(response)
            response
        } catch (error: Throwable) {
            options.debug(error)
            throw error
        }
    }
}


