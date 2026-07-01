package org.myorg.sut

import aws.sdk.kotlin.services.dynamodb.DynamoDbClient
import aws.sdk.kotlin.services.dynamodb.model.*
import io.github.huherto.awsLambdaStream.sinks.DynamoDbUpdateValue
import io.github.huherto.awsLambdaStream.sinks.updateExpression

class Connector(
    debug: (String) -> Unit,
    tableName: String?,
    timeout: Long = System.getenv("DYNAMODB_TIMEOUT")?.toLongOrNull()
        ?: System.getenv("TIMEOUT")?.toLongOrNull()
        ?: 1_000L,
    private val db: DynamoDbClient,
) {
    private val debug: (Any?) -> Unit = { msg -> debug(msg.toString()) }
    private val tableName: String = tableName ?: "undefined"

    suspend fun update(
        key: Map<String, AttributeValue>,
        inputParams: Map<String, DynamoDbUpdateValue>,
    ): UpdateItemResponse {
        val expression = updateExpression(inputParams)

        val request = UpdateItemRequest {
            tableName = this@Connector.tableName
            this.key = key
            expressionAttributeNames = expression.expressionAttributeNames
            expressionAttributeValues = expression.expressionAttributeValues
            updateExpression = expression.updateExpression
            returnValues = ReturnValue.AllNew
        }

        return runCatching {
            db.updateItem(request)
        }.onSuccess {
            debug(it)
        }.onFailure {
            debug(it)
        }.getOrThrow()
    }

    suspend fun get(id: String): List<Map<String, AttributeValue>> {
        val request = QueryRequest {
            tableName = this@Connector.tableName
            limit = 3
            scanIndexForward = false
            keyConditionExpression = "#pk = :pk"
            expressionAttributeNames = mapOf(
                "#pk" to "pk",
            )
            expressionAttributeValues = mapOf(
                ":pk" to AttributeValue.S(id),
            )
            consistentRead = true
        }

        return runCatching {
            db.query(request)
        }.onSuccess {
            debug(it)
        }.onFailure {
            debug(it)
        }.getOrThrow()
            .items
            .orEmpty()
    }
}