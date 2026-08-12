package org.myorg.sut.facades

import aws.sdk.kotlin.services.dynamodb.DynamoDbClient
import aws.sdk.kotlin.services.dynamodb.model.AttributeValue
import aws.sdk.kotlin.services.dynamodb.model.QueryRequest
import kotlinx.coroutines.delay
import kotlinx.datetime.Clock
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
        debug: Boolean = false,
        checkResponse: (List<DBRecord>?) -> DBRecord?,
    ): DBRecord? {
        val startTime = Clock.System.now().toEpochMilliseconds()

        while (true) {
            if (Clock.System.now().toEpochMilliseconds() - startTime > 30000) {
                logger.error { "Timed out waiting for event $pk to be inserted." }
                return null
            }

            logger.debug { "find event $pk in ${Clock.System.now().toEpochMilliseconds() - startTime}ms" }

            val response = client.query(QueryRequest {
                tableName = eventTableName()
                keyConditionExpression = "pk = :pk"
                expressionAttributeValues = mapOf(":pk" to AttributeValue.S(pk))
            })

            if (debug) {
                logger.debug {
                    "Query event table ${eventTableName()} by pk=$pk returned ${response.items?.size ?: 0} item(s): ${response.items}"
                }
            }

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
        val startTime = Clock.System.now().toEpochMilliseconds()

        while (true) {
            if (Clock.System.now().toEpochMilliseconds() - startTime > 10000) {
                logger.error { "Timed out waiting for entity $pk to be inserted." }
                return null
            }

            logger.debug { "find entity $pk in ${Clock.System.now().toEpochMilliseconds() - startTime}" }

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