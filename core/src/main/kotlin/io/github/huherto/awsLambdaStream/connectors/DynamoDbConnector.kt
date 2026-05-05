package io.github.huherto.awsLambdaStream.connectors

import aws.sdk.kotlin.runtime.auth.credentials.EnvironmentCredentialsProvider
import aws.sdk.kotlin.services.dynamodb.DynamoDbClient
import aws.sdk.kotlin.services.dynamodb.model.*
import io.github.huherto.awsLambdaStream.EnvironmentConfig
import io.github.huherto.awsLambdaStream.UnitOfWork

interface DynamoDbConnectorMetrics {
    suspend fun capture(
        client: DynamoDbClient,
        operation: String,
        service: String,
        options: DynamoDbConnectorOptions,
        ctx: Any?,
    )
}

data class DynamoDbConnectorOptions(
    val envConfig: EnvironmentConfig,
    val debug: (Any?) -> Unit = {},
    val tableName: String = "undefined",
    val pipelineId: String = "default",
    val timeoutMillis: Long =
        envConfig.dynamodbTimeout()
            ?: envConfig.timeout()
            ?: 1_000L,
    val retryConfig: RetryConfig = RetryConfig(),
    val throwConditionFailure: Boolean = false,
    val metrics: DynamoDbConnectorMetrics? = null,
    val region: String? = envConfig.awsRegion() ?: envConfig.awsDefaultRegion(),
)

class DynamoDbConnector(
    private val options: DynamoDbConnectorOptions
) {

    private val tableName: String = options.tableName
    private val retryConfig: RetryConfig = options.retryConfig
    private val throwConditionFailure: Boolean = options.throwConditionFailure

    private val client: DynamoDbClient =
        getClient(
            pipelineId = options.pipelineId,
            region = options.region,
        )

    private fun batchGetRetryExecutor(ctx: Any?) =
        RetryExecutor(
            retryConfig = RetryConfig(
                maxRetries = retryConfig.maxRetries,
                retryWait = retryConfig.retryWait
            ),
            strategy = DynamoDbBatchGetRetryStrategy(),
            send = { request: BatchGetItemRequest ->
                sendCommand(
                    operation = "BatchGetItem",
                    ctx = ctx,
                ) {
                    client.batchGetItem(request)
                }
            },
        )

    suspend fun update(
        updateRequest: UpdateItemRequest,
        uow: UnitOfWork,
    ): UpdateItemResponse? {

        return try {
            sendCommand(
                operation = "UpdateItem",
                ctx = uow,
            ) {
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

        return sendCommand(
            operation = "PutItem",
            ctx = uow,
        ) {
            client.putItem(putRequest)
        }
    }

    suspend fun batchGet(
        requestItems: Map<String, KeysAndAttributes>,
        ctx: Any? = null,
    ): BatchGetItemResponse {
        val request = BatchGetItemRequest {
            this.requestItems = requestItems
        }

        return batchGetRetryExecutor(ctx).execute(request)
    }

    suspend fun batchGet(
        request: BatchGetItemRequest,
        ctx: Any? = null,
    ): BatchGetItemResponse {
        return batchGetRetryExecutor(ctx).execute(request)
    }

    suspend fun query(
        request: QueryRequest,
        ctx: Any? = null,
    ): List<Map<String, AttributeValue>> {
        return queryAll(request, ctx)
    }

    suspend fun queryAll(
        request: QueryRequest,
        ctx: Any? = null,
    ): List<Map<String, AttributeValue>> {
        val baseRequest = request.withTableNameIfMissing(tableName)
        val items = mutableListOf<Map<String, AttributeValue>>()

        var cursor: Map<String, AttributeValue>? = null

        do {
            val pageRequest = baseRequest.copy {
                exclusiveStartKey = cursor
            }

            val response = sendCommand(
                operation = "Query",
                ctx = ctx,
            ) {
                client.query(pageRequest)
            }

            items += response.items.orEmpty()

            val limit = baseRequest.limit
            val lastEvaluatedKey = response.lastEvaluatedKey
            val shouldContinue =
                !lastEvaluatedKey.isNullOrEmpty() &&
                    (limit == null || items.size < limit)

            cursor = if (shouldContinue) {
                lastEvaluatedKey
            } else {
                null
            }
        } while (cursor != null)

        return if (baseRequest.limit != null) {
            items.take(baseRequest.limit!!)
        } else {
            items
        }
    }

    suspend fun queryPage(
        request: QueryRequest,
        ctx: Any? = null,
    ): QueryResponse {
        return sendCommand(
            operation = "Query",
            ctx = ctx,
        ) {
            client.query(request.withTableNameIfMissing(tableName))
        }
    }

    suspend fun scan(
        request: ScanRequest,
        ctx: Any? = null,
    ): ScanResponse {
        return sendCommand(
            operation = "Scan",
            ctx = ctx,
        ) {
            client.scan(request.withTableNameIfMissing(tableName))
        }
    }

    private suspend fun <T> sendCommand(
        operation: String,
        ctx: Any?,
        block: suspend () -> T,
    ): T {
        options.metrics?.capture(
            client = client,
            operation = operation,
            service = "dynamodb",
            options = options,
            ctx = ctx,
        )

        return try {
            val response = block()
            options.debug(response)
            response
        } catch (error: Throwable) {
            options.debug(error)
            throw error
        }
    }

    companion object {
        private val clients = mutableMapOf<String, DynamoDbClient>()

        private fun getClient(
            pipelineId: String,
            region: String?,
        ): DynamoDbClient {
            return synchronized(clients) {
                clients.getOrPut(pipelineId) {
                    DynamoDbClient {
                        if (!region.isNullOrBlank()) {
                            this.region = region
                        }

                        credentialsProvider = EnvironmentCredentialsProvider()
                    }
                }
            }
        }
    }
}

private fun UpdateItemRequest.withTableNameIfMissing(
    tableName: String,
): UpdateItemRequest {
    return if (this.tableName.isNullOrBlank()) {
        copy {
            this.tableName = tableName
        }
    } else {
        this
    }
}

private fun PutItemRequest.withTableNameIfMissing(
    tableName: String,
): PutItemRequest {
    return if (this.tableName.isNullOrBlank()) {
        copy {
            this.tableName = tableName
        }
    } else {
        this
    }
}

private fun QueryRequest.withTableNameIfMissing(
    tableName: String,
): QueryRequest {
    return if (this.tableName.isNullOrBlank()) {
        copy {
            this.tableName = tableName
        }
    } else {
        this
    }
}

private fun ScanRequest.withTableNameIfMissing(
    tableName: String,
): ScanRequest {
    return if (this.tableName.isNullOrBlank()) {
        copy {
            this.tableName = tableName
        }
    } else {
        this
    }
}



