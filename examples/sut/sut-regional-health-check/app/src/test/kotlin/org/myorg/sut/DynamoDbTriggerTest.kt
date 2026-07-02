package org.myorg.sut

import aws.sdk.kotlin.services.s3.model.PutObjectRequest
import aws.sdk.kotlin.services.s3.model.PutObjectResponse
import com.amazonaws.services.lambda.runtime.Context
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent
import com.amazonaws.services.lambda.runtime.events.models.dynamodb.StreamRecord
import io.github.huherto.awsLambdaStream.EnvironmentConfig
import io.github.huherto.awsLambdaStream.FaultManager
import io.github.huherto.awsLambdaStream.connectors.S3Connector
import io.github.huherto.awsLambdaStream.sinks.EventPublisherInMemory
import io.kotest.matchers.shouldBe
import io.mockk.*
import org.junit.jupiter.api.Test
import java.util.*
import com.amazonaws.services.lambda.runtime.events.models.dynamodb.AttributeValue as DynamoDbEventAttributeValue

class DynamoDbTriggerTest {

    fun mockEnvConfig() : EnvironmentConfig {
        val envConfig: EnvironmentConfig = spyk()
        coEvery { envConfig.awsRegion() } returns "eu-west-1"
        coEvery { envConfig.bucketName() } returns "bucket-name"
        return envConfig
    }

    @Test
    fun `handleRequest should process dynamodb records through real container and put materialized events in S3`() {
        // Arrange
        val putRequestSlot = slot<PutObjectRequest>()
        val s3Connector: S3Connector = mockk(relaxed = true)
        val envConfig = mockEnvConfig()
        coEvery {
            s3Connector.putObject(capture(putRequestSlot), any())
        } returns PutObjectResponse {}

        val container = DynamoDbTriggerContainer(
            faultManager = FaultManager(
                envConfig = envConfig,
                eventPublisher = EventPublisherInMemory(),
                skipErrorLogging = false,
                isStreamRetryEnabled = false,
            ),
            s3Connector = s3Connector,
            envConfig = envConfig
        )

        val trigger = DynamoDbTrigger(container)
        val context: Context = mockk(relaxed = true)
        val ddbEvent = dynamoDbEvent(
            dynamoDbRecord(
                eventId = "event-1",
                eventName = "INSERT",
                sequenceNumber = "1",
                pk = "health-check#eu-west-1",
                sk = "regional-health-check",
                data = """{"status":"STARTED"}""",
            ),
            dynamoDbRecord(
                eventId = "event-2",
                eventName = "MODIFY",
                sequenceNumber = "2",
                pk = "health-check#eu-west-1",
                sk = "regional-health-check",
                data = """{"status":"COMPLETED"}""",
            ),
        )

        // Act
        val result = trigger.handleRequest(ddbEvent, context)

        // Assert
        result shouldBe "Done"

        coVerify(exactly = 2) {
            s3Connector.putObject(any(), any())
        }
    }

    @Test
    fun `handleRequest should not put anything in S3 when there are no dynamodb records`() {
        // Arrange
        val s3Connector: S3Connector = mockk(relaxed = true)

        val envConfig = mockEnvConfig()
        val container = DynamoDbTriggerContainer(
            faultManager = FaultManager(
                envConfig = envConfig,
                eventPublisher = EventPublisherInMemory(),
                skipErrorLogging = true,
                isStreamRetryEnabled = false,
            ),
            s3Connector = s3Connector,
            envConfig = envConfig
        )

        val trigger = DynamoDbTrigger(container)
        val context: Context = mockk(relaxed = true)
        val ddbEvent = dynamoDbEvent()

        // Act
        val result = trigger.handleRequest(ddbEvent, context)

        // Assert
        result shouldBe "Done"

        coVerify(exactly = 0) {
            s3Connector.putObject(any(), any())
        }
    }

    private fun dynamoDbEvent(
        vararg records: DynamodbEvent.DynamodbStreamRecord,
    ): DynamodbEvent {
        return DynamodbEvent().apply {
            this.records = records.toList()
        }
    }

    private fun dynamoDbRecord(
        eventId: String,
        eventName: String,
        sequenceNumber: String,
        pk: String,
        sk: String,
        data: String,
    ): DynamodbEvent.DynamodbStreamRecord {
        return DynamodbEvent.DynamodbStreamRecord().apply {
            this.eventID = eventId
            this.eventName = eventName
            this.awsRegion = "eu-west-1"
            this.dynamodb = StreamRecord().apply {
                this.sequenceNumber = sequenceNumber
                this.approximateCreationDateTime = Date(1_700_000_000L)
                this.keys = mapOf(
                    "pk" to DynamoDbEventAttributeValue().withS(pk),
                    "sk" to DynamoDbEventAttributeValue().withS(sk),
                )
                this.newImage = mapOf(
                    "pk" to DynamoDbEventAttributeValue().withS(pk),
                    "sk" to DynamoDbEventAttributeValue().withS(sk),
                    "discriminator" to DynamoDbEventAttributeValue().withS("regional-health-check"),
                    "data" to DynamoDbEventAttributeValue().withS(data),
                    "timestamp" to DynamoDbEventAttributeValue().withN("1700000000"),
                )
            }
        }
    }
}