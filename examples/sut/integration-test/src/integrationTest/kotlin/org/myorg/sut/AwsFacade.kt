package org.myorg.sut

import aws.sdk.kotlin.runtime.auth.credentials.StaticCredentialsProvider
import aws.sdk.kotlin.services.dynamodb.DynamoDbClient
import aws.sdk.kotlin.services.dynamodb.model.AttributeValue
import aws.sdk.kotlin.services.dynamodb.model.QueryRequest
import aws.sdk.kotlin.services.eventbridge.EventBridgeClient
import aws.sdk.kotlin.services.eventbridge.model.PutEventsRequest
import aws.sdk.kotlin.services.eventbridge.model.PutEventsRequestEntry
import aws.sdk.kotlin.services.kinesis.KinesisClient
import aws.sdk.kotlin.services.kinesis.model.GetRecordsRequest
import aws.sdk.kotlin.services.kinesis.model.GetShardIteratorRequest
import aws.sdk.kotlin.services.kinesis.model.ShardIteratorType
import aws.sdk.kotlin.services.s3.S3Client
import aws.sdk.kotlin.services.s3.listObjectsV2
import aws.sdk.kotlin.services.s3.model.GetObjectRequest
import aws.sdk.kotlin.services.sqs.SqsClient
import aws.sdk.kotlin.services.sqs.model.GetQueueUrlRequest
import aws.sdk.kotlin.services.sqs.model.ReceiveMessageRequest
import aws.smithy.kotlin.runtime.content.decodeToString
import aws.smithy.kotlin.runtime.net.url.Url
import io.github.huherto.awsLambdaStream.Event
import io.kotest.matchers.shouldBe
import kotlinx.coroutines.delay
import mu.KotlinLogging
import kotlin.time.Duration.Companion.milliseconds

class AwsFacade(val entityTable : String? = null, val eventTable : String? = null) {

    private val logger = KotlinLogging.logger {  }

    fun endPointUrl() = Url.parse("http://localhost:4566")

    fun entityTableName() : String {
        return entityTable ?: error("entityTable is required")
    }

    fun eventTableName() : String {
        return eventTable ?: error("eventTable is required")
    }

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

    private val eventBridgeClient: EventBridgeClient by lazy {
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

    private val s3Client: S3Client by lazy {
        S3Client {
            this.region = "us-east-1"
            this.endpointUrl = endPointUrl()
            this.forcePathStyle = true // Required for LocalStack S3 bucket/object operations
            credentialsProvider =
                StaticCredentialsProvider {
                    this.accessKeyId = "test"
                    this.secretAccessKey = "test"
                }
        }
    }

    private val sqsClient: SqsClient by lazy {
        SqsClient {
            this.region = "us-east-1"
            this.endpointUrl = endPointUrl()
            credentialsProvider =
                StaticCredentialsProvider {
                    this.accessKeyId = "test"
                    this.secretAccessKey = "test"
                }
        }
    }
    suspend fun putEvents(vararg events: Event) {
        val entries = events.map { event ->
            PutEventsRequestEntry {
                eventBusName = "sut-event-hub-local-bus"
                detail = event.encoded()
                detailType = event.eventType()
                source = "integration-test"
            }
        }
        val res = eventBridgeClient.putEvents(PutEventsRequest {
            this.entries = entries
        })
        res.failedEntryCount shouldBe 0
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

    suspend fun findEntityByPK(pk: String, checkResponse: (List<DBRecord>?)-> DBRecord?)  : DBRecord? {

        val startTime = System.currentTimeMillis()
        while (true) {
            if (System.currentTimeMillis() - startTime > 10000) {
                logger.error { "Timed out waiting for event $pk to be inserted." }
                return null
            }
            logger.debug { "find entity $pk in ${System.currentTimeMillis() - startTime}" }
            val response = dynamoDbClient.query(QueryRequest {
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
        s3Client.close()
        sqsClient.close()
    }

    suspend fun verifyFaultEventStoredInS3(faultId: String) : String? {

        val startTime = System.currentTimeMillis()
        while (true) {
            if (System.currentTimeMillis() - startTime > 10000) {
                logger.error { "Timed out waiting for s3 object with faultId: $faultId to be inserted." }
                return null
            }
            val bucketName = "myorg-sut-event-fault-monitor-local-us-east-1"
            val response = s3Client.listObjectsV2 {
                this.bucket = bucketName
            }

            val contents = response.contents.orEmpty()

            logger.info {
                "S3 list response: keyCount=${response.keyCount}, " +
                        "isTruncated=${response.isTruncated}, " +
                        "contents=${contents.map { it.key }}"
            }

            val keys = contents.sortedBy { it.key }.map { it.key }.reversed()

            keys.take(5).forEach { key ->
                logger.info { "S3 object key: $key" }
                val content = s3Client.getObject(GetObjectRequest {
                    this.bucket = bucketName
                    this.key = key
                }) { response ->
                    val content = response.body?.decodeToString()
                    logger.info { "S3 object content: $content" }
                    content
                }
                if (content != null && content.contains(faultId)) {
                    logger.info { "Fault found in S3 object: $key" }
                    return content
                }
            }
            delay(1000.milliseconds)
        }
    }

    suspend fun verifyNotificationSentToSns(
        queueName: String,
        expectedContent: String,
    ): String? {
        val queueUrl = sqsClient.getQueueUrl(GetQueueUrlRequest {
            this.queueName = queueName
        }).queueUrl ?: error("Queue URL not found for queue: $queueName")

        val startTime = System.currentTimeMillis()

        while (true) {
            if (System.currentTimeMillis() - startTime > 10000) {
                logger.error { "Timed out waiting for SNS notification containing: $expectedContent" }
                return null
            }

            val response = sqsClient.receiveMessage(ReceiveMessageRequest {
                this.queueUrl = queueUrl
                this.maxNumberOfMessages = 10
                this.waitTimeSeconds = 1
            })

            response.messages.orEmpty().forEach { message ->
                val body = message.body

                logger.info { "Received SNS/SQS message" }

                if (body != null && body.contains(expectedContent)) {
                    return body
                }
            }

            delay(1000.milliseconds)
        }
    }

}