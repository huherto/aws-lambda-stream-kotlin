package io.github.huherto.awsLambdaStream.connectors

import aws.sdk.kotlin.runtime.auth.credentials.EnvironmentCredentialsProvider
import aws.sdk.kotlin.services.dynamodb.DynamoDbClient
import aws.sdk.kotlin.services.dynamodb.model.*
import aws.smithy.kotlin.runtime.net.url.Url
import io.github.huherto.awsLambdaStream.EnvironmentConfig
import io.github.huherto.awsLambdaStream.UnitOfWork

interface DynamoDbClientFactory : ClientFactory<DynamoDbClient>

class DefaultDynamoDbClientFactory(private val envConfig: EnvironmentConfig) : DynamoDbClientFactory, AbstractClientFactory<DynamoDbClient>() {
    override fun create(): DynamoDbClient {
        val endpointUrl = envConfig.endPointUrl()
        val region = envConfig.awsRegion()
        return DynamoDbClient {
            this.region = region
            this.credentialsProvider = EnvironmentCredentialsProvider()

            // If an endpoint URL is provided (like http://localhost:4566), use it
            endpointUrl?.let { this.endpointUrl = Url.parse(it) }

        }
    }
}

class DynamoDbConnector(
    val debug: (Any?) -> Unit = {},
    val throwConditionFailure: Boolean = false,
    private val dynamoDbClientFactory: DynamoDbClientFactory,
    private val retryConfig: RetryConfig = RetryConfig(),
) {

    fun getClient(uow: UnitOfWork): DynamoDbClient {
        val pipelineId = uow.pipeline?.id ?: "unknown"
        return dynamoDbClientFactory.getClient(pipelineId)
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
            if (throwConditionFailure) {
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
            retryConfig = retryConfig,
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
            debug(response)
            response
        } catch (error: Throwable) {
            debug(error)
            throw error
        }
    }
}


