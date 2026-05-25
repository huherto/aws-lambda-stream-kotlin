package io.github.huherto.awsLambdaStream.flavors

import aws.sdk.kotlin.services.dynamodb.model.AttributeValue
import aws.sdk.kotlin.services.dynamodb.model.BatchGetItemRequest
import aws.sdk.kotlin.services.dynamodb.model.KeysAndAttributes
import aws.sdk.kotlin.services.dynamodb.model.QueryRequest
import io.github.huherto.awsLambdaStream.BaseEvent
import io.github.huherto.awsLambdaStream.Event
import io.github.huherto.awsLambdaStream.UnitOfWork
import io.github.huherto.awsLambdaStream.connectors.DynamoDbConnector
import io.github.huherto.awsLambdaStream.queries.QueryRule
import io.github.huherto.awsLambdaStream.sinks.EventPublisher
import io.kotest.matchers.collections.shouldContainExactly
import io.kotest.matchers.maps.shouldContainExactly
import io.kotest.matchers.nulls.shouldBeNull
import io.kotest.matchers.shouldBe
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test

class CdcPipelineTest {

    private val dynamoDbConnector = mockk<DynamoDbConnector>(relaxed = true)
    private val eventPublisher = mockk<EventPublisher>()

    private fun createPipeline(
        toQueryRequest: (suspend (UnitOfWork) -> QueryRequest?)? = null,
        queryRule: QueryRule? = null,
        toBatchGetRequest: (suspend (UnitOfWork) -> BatchGetItemRequest?)? = null,
        toEvent: (suspend (UnitOfWork) -> Event?)? = null,
        encryptEvent: (suspend (UnitOfWork) -> UnitOfWork)? = null,
    ): CdcPipeline {
        return CdcPipeline(
            id = "cdc-pipeline",
            dynamoDbConnector = dynamoDbConnector,
            eventPublisher = eventPublisher,
            toQueryRequest = toQueryRequest,
            queryRule = queryRule,
            toBatchGetRequest = toBatchGetRequest,
            toEvent = toEvent,
            encryptEvent = encryptEvent,
        )
    }

    private fun createEvent(
        id: String = "event-1",
        partitionKey: String? = "partition-1",
        type: String = "TestEvent",
    ): Event = object : BaseEvent() {
        override var id: String? = id
        override var partitionKey: String? = partitionKey

        override fun eventType(): String = type

        override fun encoded(): String = """{"id":"$id","type":"$type"}"""
    }

    @Test
    fun `addQueryRequest should use custom mapper when provided`() = runTest {
        // Arrange
        val expectedRequest = QueryRequest {
            tableName = "custom-table"
            keyConditionExpression = "#pk = :pk"
            expressionAttributeNames = mapOf("#pk" to "customPk")
            expressionAttributeValues = mapOf(":pk" to AttributeValue.S("custom-partition"))
        }
        val original = UnitOfWork(event = createEvent())

        val pipeline = createPipeline(
            queryRule = QueryRule(pkFn = "ignoredPk"),
            toQueryRequest = { uow ->
                uow shouldBe original
                expectedRequest
            },
        )

        // Act
        val result = pipeline.addQueryRequest(original)

        // Assert
        result.queryRequest shouldBe expectedRequest
        result.event shouldBe original.event
    }

    @Test
    fun `addQueryRequest should create default primary key query when query rule exists`() = runTest {
        // Arrange
        val event = createEvent(partitionKey = "partition-1")
        val original = UnitOfWork(event = event)
        val pipeline = createPipeline(
            queryRule = QueryRule(pkFn = "accountId"),
        )

        // Act
        val result = pipeline.addQueryRequest(original)

        // Assert
        val queryRequest = result.queryRequest
        queryRequest?.keyConditionExpression shouldBe "#pk = :pk"
        queryRequest?.expressionAttributeNames?.shouldContainExactly(mapOf("#pk" to "accountId"))
        queryRequest?.expressionAttributeValues?.shouldContainExactly(mapOf(
            ":pk" to AttributeValue.S("partition-1"),
        ))
        queryRequest?.consistentRead shouldBe true
    }

    @Test
    fun `addQueryRequest should return null query request when no mapper or query rule exists`() = runTest {
        // Arrange
        val original = UnitOfWork(event = createEvent())
        val pipeline = createPipeline()

        // Act
        val result = pipeline.addQueryRequest(original)

        // Assert
        result.queryRequest.shouldBeNull()
        result.event shouldBe original.event
    }

    @Test
    fun `addBatchGetRequest should add mapped request and leave it null when mapper is missing`() = runTest {
        // Arrange
        val expectedRequest = BatchGetItemRequest {
            requestItems = mapOf(
                "events" to KeysAndAttributes {
                    keys = listOf(
                        mapOf("pk" to AttributeValue.S("event-1")),
                    )
                },
            )
        }
        val original = UnitOfWork(event = createEvent())

        val pipelineWithMapper = createPipeline(
            toBatchGetRequest = { uow ->
                uow shouldBe original
                expectedRequest
            },
        )
        val pipelineWithoutMapper = createPipeline()

        // Act
        val mappedResult = pipelineWithMapper.addBatchGetRequest(original)
        val unmappedResult = pipelineWithoutMapper.addBatchGetRequest(original)

        // Assert
        mappedResult.batchGetRequest shouldBe expectedRequest
        mappedResult.event shouldBe original.event

        unmappedResult.batchGetRequest.shouldBeNull()
        unmappedResult.event shouldBe original.event
    }

    @Test
    fun `addEvent should add mapped event and pass through unchanged when mapper is missing`() = runTest {
        // Arrange
        val originalEvent = createEvent(id = "original-event", type = "OriginalEvent")
        val mappedEvent = createEvent(id = "mapped-event", type = "MappedEvent")
        val original = UnitOfWork(event = originalEvent)

        val pipelineWithMapper = createPipeline(
            toEvent = { uow ->
                uow shouldBe original
                mappedEvent
            },
        )
        val pipelineWithoutMapper = createPipeline()

        // Act
        val mappedResult = pipelineWithMapper.addEvent(original)
        val passthroughResult = pipelineWithoutMapper.addEvent(original)

        // Assert
        mappedResult.event shouldBe mappedEvent
        passthroughResult shouldBe original
    }

    @Test
    fun `encrypt should transform unit of work and pass through unchanged when mapper is missing`() = runTest {
        // Arrange
        val originalEvent = createEvent(id = "original-event")
        val encryptedEvent = createEvent(id = "encrypted-event")
        val original = UnitOfWork(event = originalEvent)
        val encrypted = original.copy(event = encryptedEvent)

        val pipelineWithMapper = createPipeline(
            encryptEvent = { uow ->
                uow shouldBe original
                encrypted
            },
        )
        val pipelineWithoutMapper = createPipeline()

        // Act
        val encryptedResult = pipelineWithMapper.encrypt(original)
        val passthroughResult = pipelineWithoutMapper.encrypt(original)

        // Assert
        encryptedResult shouldBe encrypted
        passthroughResult shouldBe original
    }

    @Test
    fun `publish should delegate to configured event publisher`() = runTest {
        // Arrange
        val first = UnitOfWork(event = createEvent(id = "event-1"))
        val second = UnitOfWork(event = createEvent(id = "event-2"))
        val inputFlow = flowOf(first, second)
        val publishedFlow = flowOf(second, first)

        every { eventPublisher.publish(inputFlow) } returns publishedFlow

        val pipeline = createPipeline()

        // Act
        val result = pipeline.run {
            inputFlow.publish().toList()
        }

        // Assert
        result shouldContainExactly listOf(second, first)
        verify(exactly = 1) {
            eventPublisher.publish(inputFlow)
        }
    }
}