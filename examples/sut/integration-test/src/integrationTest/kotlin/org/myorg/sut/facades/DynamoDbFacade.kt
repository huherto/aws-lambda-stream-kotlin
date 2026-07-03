package org.myorg.sut.facades

import aws.sdk.kotlin.services.dynamodb.DynamoDbClient
import aws.sdk.kotlin.services.dynamodb.model.AttributeValue
import aws.sdk.kotlin.services.dynamodb.model.QueryRequest
import kotlinx.coroutines.delay
import mu.KotlinLogging
import org.myorg.sut.DBRecord
import kotlin.time.Duration.Companion.milliseconds

class DynamoDbFacade(
    private val entityTable: String? = null,
    private val eventTable: String? = null,
    private val config: AwsLocalConfig = AwsLocalConfig(),
) {
    private val logger = KotlinLogging.logger {}

    val client: DynamoDbClient by lazy {
        DynamoDbClient {
            region = config.region
            endpointUrl = config.endpointUrl
            credentialsProvider = config.credentialsProvider()
        }
    }

    fun entityTableName(): String =
        entityTable ?: error("entityTable is required")

    fun eventTableName(): String =
        eventTable ?: error("eventTable is required")

    suspend fun findEventByPK(
        pk: String,
        checkResponse: (List<DBRecord>?) -> DBRecord?,
    ): DBRecord? {
        val startTime = System.currentTimeMillis()

        while (true) {
            if (System.currentTimeMillis() - startTime > 10000) {
                logger.error { "Timed out waiting for event $pk to be inserted." }
                return null
            }

            logger.debug { "find event $pk in ${System.currentTimeMillis() - startTime}" }

            val response = client.query(QueryRequest {
                tableName = eventTableName()
                keyConditionExpression = "pk = :pk"
                expressionAttributeValues = mapOf(":pk" to AttributeValue.S(pk))
            })

            val found = checkResponse(response.items)
            if (found != null) {
                return found
            }

            delay(1000.milliseconds)
        }
    }

    suspend fun findEntityByPK(
        pk: String,
        checkResponse: (List<DBRecord>?) -> DBRecord?,
    ): DBRecord? {
        val startTime = System.currentTimeMillis()

        while (true) {
            if (System.currentTimeMillis() - startTime > 10000) {
                logger.error { "Timed out waiting for entity $pk to be inserted." }
                return null
            }

            logger.debug { "find entity $pk in ${System.currentTimeMillis() - startTime}" }

            val response = client.query(QueryRequest {
                tableName = entityTableName()
                keyConditionExpression = "pk = :pk"
                expressionAttributeValues = mapOf(":pk" to AttributeValue.S(pk))
            })

            val found = checkResponse(response.items)
            if (found != null) {
                return found
            }

            delay(1000.milliseconds)
        }
    }

    fun close() {
        client.close()
    }
}