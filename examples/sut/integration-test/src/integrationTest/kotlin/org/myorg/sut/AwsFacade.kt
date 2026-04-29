package org.myorg.sut

import aws.sdk.kotlin.runtime.auth.credentials.StaticCredentialsProvider
import aws.sdk.kotlin.services.dynamodb.DynamoDbClient
import aws.sdk.kotlin.services.dynamodb.model.AttributeValue
import aws.sdk.kotlin.services.dynamodb.model.QueryRequest
import aws.sdk.kotlin.services.eventbridge.EventBridgeClient
import aws.sdk.kotlin.services.kinesis.KinesisClient
import aws.sdk.kotlin.services.kinesis.model.GetRecordsRequest
import aws.sdk.kotlin.services.kinesis.model.GetShardIteratorRequest
import aws.sdk.kotlin.services.kinesis.model.ShardIteratorType
import aws.smithy.kotlin.runtime.net.url.Url
import kotlinx.coroutines.delay
import kotlin.time.Duration.Companion.milliseconds

object AwsFacade {

    private val logger = mu.KotlinLogging.logger {  }

    fun endPointUrl() = Url.parse("http://localhost:4566")

    private val dynamoDbClient: DynamoDbClient by lazy {
        DynamoDbClient {
            this.region = "us-east-1"
            this.endpointUrl = endPointUrl()
            credentialsProvider =
                StaticCredentialsProvider {
                    this.accessKeyId = "test"
                    this.secretAccessKey = "test"
                }
        }
    }

    val eventBridgeClient: EventBridgeClient by lazy {
        EventBridgeClient {
            this.region = "us-east-1"
            this.endpointUrl = endPointUrl()
            credentialsProvider =
                StaticCredentialsProvider {
                    this.accessKeyId = "test"
                    this.secretAccessKey = "test"
                }
        }
    }

    private val kinesisClient: KinesisClient by lazy {
        KinesisClient {
            this.region = "us-east-1"
            this.endpointUrl = endPointUrl()
            credentialsProvider =
                StaticCredentialsProvider {
                    this.accessKeyId = "test"
                    this.secretAccessKey = "test"
                }
        }
    }

    suspend fun findEventByPK(pk: String, checkResponse: (List<DBRecord>?)-> DBRecord?)  : DBRecord? {

        val startTime = System.currentTimeMillis()
        while (true) {
            if (System.currentTimeMillis() - startTime > 10000) {
                logger.error { "Timed out waiting for event $pk to be inserted." }
                return null
            }
            logger.debug { "find event $pk in ${System.currentTimeMillis() - startTime}" }
            val response = dynamoDbClient.query(QueryRequest {
                tableName = "sut-control-service-local-events"
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

    suspend fun readAllKinesisEvents(): List<String> {
        val streamName = "sut-event-hub-local-s1"
        val events = mutableListOf<String>()

        val shardIteratorResponse = kinesisClient.getShardIterator(GetShardIteratorRequest {
            this.streamName = streamName
            this.shardId = "shardId-000000000000"
            this.shardIteratorType = ShardIteratorType.TrimHorizon
        })

        var shardIterator = shardIteratorResponse.shardIterator

        while (shardIterator != null) {
            val recordsResponse = kinesisClient.getRecords(GetRecordsRequest {
                this.shardIterator = shardIterator
            })

            recordsResponse.records.forEach { record ->
                val data = record.data.decodeToString()
                events.add(data)
            }

            shardIterator = recordsResponse.nextShardIterator

            if (recordsResponse.records.isEmpty()) {
                break
            }
        }

        return events
    }

    fun closeAll() {
        dynamoDbClient.close()
        eventBridgeClient.close()
        kinesisClient.close()
    }

}