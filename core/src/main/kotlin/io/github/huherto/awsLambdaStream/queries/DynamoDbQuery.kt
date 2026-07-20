package io.github.huherto.awsLambdaStream.queries

import aws.sdk.kotlin.services.dynamodb.model.AttributeValue
import aws.sdk.kotlin.services.dynamodb.model.BatchGetItemResponse
import aws.sdk.kotlin.services.dynamodb.model.QueryResponse
import io.github.huherto.awsLambdaStream.EnvironmentConfig
import io.github.huherto.awsLambdaStream.FaultManager
import io.github.huherto.awsLambdaStream.UnitOfWork
import io.github.huherto.awsLambdaStream.connectors.DefaultDynamoDbClientFactory
import io.github.huherto.awsLambdaStream.connectors.DynamoDbConnector
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.Flow
import java.util.concurrent.ConcurrentHashMap

class DynamoDbQuery(
    private val envConfig: EnvironmentConfig,
    private val connector: DynamoDbConnector? = null,
    private val decrypt: MapDecryptFunc? = null
) {

    val memoryCache = ConcurrentHashMap<String, Any>()

    fun getConnector()  : DynamoDbConnector {
        if (connector != null) {
            return connector
        }
        return DynamoDbConnector(dynamoDbClientFactory = DefaultDynamoDbClientFactory(envConfig))
    }

    fun queryAllDynamoDB(
        fm: FaultManager,
        source: Flow<UnitOfWork>
    ): Flow<UnitOfWork> {
        return fm.mapNotFaultyFrom(source) { uow ->
            val request = uow.queryRequest ?: return@mapNotFaultyFrom uow

            val reqKey = request.toString()
            var cachedResponse = memoryCache[reqKey] as? QueryResponse
            if (cachedResponse == null) {
                cachedResponse = getConnector().queryAll(request, uow)
                memoryCache[reqKey] = cachedResponse
            }
            uow.copy(queryResponse = cachedResponse)
        }
    }

    fun batchGetDynamoDB(
        fm: FaultManager,
        source: Flow<UnitOfWork>,
    ): Flow<UnitOfWork>  {
        return fm.mapNotFaultyFrom(source) { uow ->
            val request = uow.batchGetRequest ?: return@mapNotFaultyFrom uow

            val reqKey = request.toString() // Or JSON serialization of request
            var cachedResponse = memoryCache[reqKey] as? BatchGetItemResponse
            if (cachedResponse == null) {
                val result = getConnector().batchGetItem(request, uow)
                val decryptedResponses = decryptResponses(result.responses, decrypt)
                cachedResponse = result.copy { responses = decryptedResponses }
                memoryCache[reqKey] = cachedResponse
            }
            uow.copy(batchGetResponse = cachedResponse)
        }
    }

    private suspend fun decryptResponses(
        responses: Map<String, List<Map<String, AttributeValue>>>?,
        decrypt: MapDecryptFunc?
    ): Map<String, List<Map<String, AttributeValue>>>? {
        if (decrypt == null) return responses
        val decryptedResponses = coroutineScope {
            responses?.mapValues { (_, items) ->
                items.map { item ->
                    async { decrypt(item) }
                }.awaitAll()
            }
        }
        return decryptedResponses
    }

}