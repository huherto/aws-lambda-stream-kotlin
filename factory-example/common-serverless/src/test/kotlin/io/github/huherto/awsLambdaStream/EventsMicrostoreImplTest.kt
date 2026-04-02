package io.github.huherto.awsLambdaStream

import aws.sdk.kotlin.services.dynamodb.DynamoDbClient
import aws.sdk.kotlin.services.dynamodb.model.AttributeValue
import io.github.huherto.awsLambdaStream.sinks.EventsMicrostore
import io.github.huherto.awsLambdaStream.sinks.EventsMicrostoreImpl
import io.kotest.matchers.nulls.shouldNotBeNull
import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.shouldBeTypeOf
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Test

class EventsMicrostoreImplTest {

    private val envConfig = mockk<EnvironmentConfig>()
    private val dynamoDbClient = mockk<DynamoDbClient>()
    private val faultManager = mockk<FaultManager>()

    private val sut = EventsMicrostoreImpl(envConfig, dynamoDbClient, faultManager)

    @Test
    fun `putRequest should correctly populate PutItemRequest based on UnitOfWork, Event, and EnvironmentConfig`() {
        // Arrange
        val eventId = "test-event-id"
        val eventTimestamp = 1672531200000L
        val eventEncoded = "{\"data\":\"encoded-event\"}"
        val awsRegion = "eu-west-1"
        val expectedTableName = "custom-events-table"

        val mockEvent = mockk<Event> {
            every { id } returns eventId
            every { timestamp } returns eventTimestamp
            every { encoded() } returns eventEncoded
        }

        val mockSaveOptions = mockk<EventsMicrostore.SaveOptions> {
            every { includeRaw } returns true
            every { ttlTimestampInSecs } returns 987654321
            every { expire } returns "expire-timestamp"
        }

        val uow = UnitOfWork(
            event = mockEvent,
            key = "uow-key",
            saveOptions = mockSaveOptions
        )

        every { envConfig.awsRegion() } returns awsRegion
        every { envConfig.tableName() } returns expectedTableName

        // Act
        val result = sut.putRequest(uow)

        // Assert
        val putRequest = result.putRequest.shouldNotBeNull()
        putRequest.tableName shouldBe expectedTableName

        val item = putRequest.item.shouldNotBeNull()
        
        item["pk"].shouldBeTypeOf<AttributeValue.S>().value shouldBe eventId
        item["sk"].shouldBeTypeOf<AttributeValue.S>().value shouldBe "EVENT"
        item["discriminator"].shouldBeTypeOf<AttributeValue.S>().value shouldBe "EVENT"
        item["timestamp"].shouldBeTypeOf<AttributeValue.N>().value shouldBe eventTimestamp.toString()
        item["awsregion"].shouldBeTypeOf<AttributeValue.S>().value shouldBe awsRegion
        item["ttl"].shouldBeTypeOf<AttributeValue.N>().value shouldBe "987654321"
        item["expire"].shouldBeTypeOf<AttributeValue.S>().value shouldBe "expire-timestamp"
        item["data"].shouldBeTypeOf<AttributeValue.S>().value shouldBe "uow-key"
        item["event"].shouldBeTypeOf<AttributeValue.S>().value shouldBe eventEncoded
    }
}