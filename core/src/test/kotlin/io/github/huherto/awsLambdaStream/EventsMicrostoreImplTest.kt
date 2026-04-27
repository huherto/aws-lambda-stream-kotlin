package io.github.huherto.awsLambdaStream

import aws.sdk.kotlin.services.dynamodb.DynamoDbClient
import aws.sdk.kotlin.services.dynamodb.model.AttributeValue
import aws.sdk.kotlin.services.dynamodb.model.QueryResponse
import io.github.huherto.awsLambdaStream.sinks.EventsMicrostore
import io.github.huherto.awsLambdaStream.sinks.EventsMicrostoreImpl
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.nulls.shouldBeNull
import io.kotest.matchers.nulls.shouldNotBeNull
import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.shouldBeTypeOf
import io.mockk.every
import io.mockk.mockk
import io.mockk.spyk
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

class EventsMicrostoreImplTest {

    private val envConfig : EnvironmentConfig by lazy {
        val spy = spyk(EnvironmentConfig())
        spy
    }
    private val dynamoDbClient = mockk<DynamoDbClient>()
    private val faultManager = mockk<FaultManager>()

    private val eventMicrostore = EventsMicrostoreImpl(envConfig, dynamoDbClient, faultManager)

    @Test
    fun `putRequest should correctly populate PutItemRequest based on UnitOfWork, Event, and EnvironmentConfig`() {
        // Arrange
        val eventId = "test-event-id"
        val eventTimestamp = 1672531200000L
        val eventEncoded = "{\"data\":\"encoded-event\"}"
        val awsRegion = "eu-west-1"
        val expectedTableName = "events"

        val mockEvent = mockk<Event> {
            every { id } returns eventId
            every { timestamp } returns eventTimestamp
            every { encoded() } returns eventEncoded
        }

        val savedOptions = EventsMicrostore.SaveOptions(
            pk = eventId,
            sk = "EVENT",
            discriminator = "EVENT",
            timeStamp = eventTimestamp.toString(),
            includeRaw = true,
            awsRegion = awsRegion,
            ttl = 987654321,
            expire = true,
            data = "uow-key",
            suffix = "",
        )

        val uow = UnitOfWork(
            event = mockEvent,
            key = "uow-key",
            saveOptions = savedOptions
        )

        // Act
        val result = eventMicrostore.putRequest(uow)

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
        item["expire"].shouldBeTypeOf<AttributeValue.Bool>().value shouldBe true
        item["data"].shouldBeTypeOf<AttributeValue.S>().value shouldBe "uow-key"
        item["event"].shouldBeTypeOf<AttributeValue.S>().value shouldBe eventEncoded
    }

    @Test
    fun `toQueryRequest should set queryRequest when correlation is true and pk is provided`() {
        // Arrange
        val uow = UnitOfWork(
            queryParams = EventsMicrostore.QueryParams(
                pk = "test-pk",
                correlation = true
            ),
            meta = mapOf("correlation" to "true", "pk" to "test-pk")
        )

        // Act
        val result = eventMicrostore.toQueryRequest(uow)

        // Assert
        val request = result.queryRequest.shouldNotBeNull()
        request.keyConditionExpression shouldBe "#pk = :pk"
        request.expressionAttributeNames shouldBe mapOf("#pk" to "pk")
        
        val pkValue = request.expressionAttributeValues?.get(":pk")
        pkValue.shouldNotBeNull()
        pkValue.shouldBeTypeOf<AttributeValue.S>().value shouldBe "test-pk"
        request.consistentRead shouldBe true
    }

    @Test
    fun `toQueryRequest should return original uow when correlation is false`() {
        // Arrange
        val uow = UnitOfWork(
            meta = mapOf("correlation" to "false", "pk" to "test-pk")
        )

        // Act
        val result = eventMicrostore.toQueryRequest(uow)

        // Assert
        result.queryRequest.shouldBeNull()
        result shouldBe uow
    }

    @Test
    fun `toQueryRequest should return original uow when pk is missing`() {
        // Arrange
        val uow = UnitOfWork(
            meta = mapOf("correlation" to "true")
        )

        // Act
        val result = eventMicrostore.toQueryRequest(uow)

        // Assert
        result.queryRequest.shouldBeNull()
        result shouldBe uow
    }

    @Test
    fun `unmarshall should return parsed JsonEvent for valid json string`() {
        // Arrange
        val jsonString = "{\"id\":\"evt-123\", \"type\":\"TEST_EVENT\"}"
        
        // Act
        val result = eventMicrostore.unmarshall(jsonString)

        // Assert
        result.id shouldBe "evt-123"
        result.eventType() shouldBe "TEST_EVENT"
    }

    @Test
    fun `unmarshall should throw exception for invalid json string`() {
        // Arrange
        val invalidJson = "invalid-json"
        
        // Act & Assert
        assertThrows<Exception> {
            eventMicrostore.unmarshall(invalidJson)
        }
    }

    @Test
    fun `toCorrelated should return original uow if queryResponse is null`() {
        // Arrange
        val uow = UnitOfWork()
        
        // Act
        val result = eventMicrostore.toCorrelated(uow)

        // Assert
        result.correlated.shouldBeNull()
        result shouldBe uow
    }

    @Test
    fun `toCorrelated should extract and parse events from queryResponse`() {
        // Arrange
        val eventJson1 = "{\"id\":\"evt-1\", \"type\":\"TYPE_1\"}"
        val eventJson2 = "{\"id\":\"evt-2\", \"type\":\"TYPE_2\"}"

        val itemsList = listOf(
            mapOf("event" to AttributeValue.S(eventJson1)),
            mapOf("event" to AttributeValue.S(eventJson2)),
            mapOf("other" to AttributeValue.S("no-event-here")) // This one should be ignored gracefully
        )

        val uow = UnitOfWork(
            queryResponse = QueryResponse {
                items = itemsList
            }
        )

        // Act
        val result = eventMicrostore.toCorrelated(uow)

        // Assert
        val correlated = result.correlated.shouldNotBeNull()
        correlated shouldHaveSize 2
        correlated[0].id shouldBe "evt-1"
        correlated[0].eventType() shouldBe "TYPE_1"
        correlated[1].id shouldBe "evt-2"
        correlated[1].eventType() shouldBe "TYPE_2"
    }
}