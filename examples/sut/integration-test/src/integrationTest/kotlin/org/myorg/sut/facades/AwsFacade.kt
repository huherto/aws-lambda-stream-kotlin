package org.myorg.sut.facades

import aws.sdk.kotlin.services.dynamodb.DynamoDbClient
import aws.sdk.kotlin.services.lambda.LambdaClient
import aws.sdk.kotlin.services.s3.S3Client
import aws.smithy.kotlin.runtime.net.url.Url
import io.github.huherto.awsLambdaStream.Event
import org.myorg.sut.DBRecord

class AwsFacade(
    entityTable: String? = null,
    eventTable: String? = null,
    private val config: AwsLocalConfig = AwsLocalConfig(),
) {
    val dynamoDb = DynamoDbFacade(
        entityTable = entityTable,
        eventTable = eventTable,
        config = config,
    )

    val eventBridge = EventBridgeFacade(config = config)
    val kinesis = KinesisFacade(config = config)
    val s3 = S3Facade(config = config)
    val sns = SnsFacade(config = config)
    val sqs = SqsFacade(config = config)
    val lambda = LambdaFacade(config = config)

    val dynamoDbClient: DynamoDbClient
        get() = dynamoDb.client

    val s3Client: S3Client
        get() = s3.client

    val lambdaClient: LambdaClient
        get() = lambda.client

    fun endPointUrl(): Url = config.endpointUrl

    fun entityTableName(): String =
        dynamoDb.entityTableName()

    fun eventTableName(): String =
        dynamoDb.eventTableName()

    suspend fun putEvents(vararg events: Event) =
        eventBridge.putEvents(*events)

    suspend fun findEventByPK(
        pk: String,
        checkResponse: (List<DBRecord>?) -> DBRecord?,
    ): DBRecord? =
        dynamoDb.findEventByPK(pk, checkResponse)

    suspend fun findEntityByPK(
        pk: String,
        checkResponse: (List<DBRecord>?) -> DBRecord?,
    ): DBRecord? =
        dynamoDb.findEntityByPK(pk, checkResponse)

    suspend fun readAllKinesisEvents(): List<String> =
        kinesis.readAllEvents()

    suspend fun publishToSnsTopic(
        topicNameContains: String,
        message: String,
        subject: String,
        messageGroupId: String,
        messageDeduplicationId: String,
    ): String =
        sns.publishToTopic(
            topicNameContains = topicNameContains,
            message = message,
            subject = subject,
            messageGroupId = messageGroupId,
            messageDeduplicationId = messageDeduplicationId,
        )

    suspend fun verifyFaultEventStoredInS3(faultId: String): String? =
        s3.verifyFaultEventStoredInS3(faultId)

    suspend fun purgeSqsQueue(queueName: String) =
        sqs.purgeQueue(queueName)

    suspend fun verifyNotificationSentToSns(
        queueName: String,
        expectedContent: String? = null,
    ): String? =
        sqs.verifyNotificationSentToSns(
            queueName = queueName,
            expectedContent = expectedContent,
        )

    fun closeAll() {
        dynamoDb.close()
        eventBridge.close()
        kinesis.close()
        s3.close()
        sns.close()
        sqs.close()
        lambda.close()
    }
}