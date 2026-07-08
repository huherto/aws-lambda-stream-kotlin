package io.github.huherto.awsLambdaStream.connectors

import aws.sdk.kotlin.services.dynamodb.DynamoDbClient
import aws.sdk.kotlin.services.dynamodb.model.*
import io.github.huherto.awsLambdaStream.FaultManager
import io.github.huherto.awsLambdaStream.UnitOfWork
import io.github.huherto.awsLambdaStream.flavors.Pipeline
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.collections.shouldContainExactly
import io.kotest.matchers.maps.shouldContainExactly
import io.kotest.matchers.shouldBe
import io.mockk.*
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import kotlin.time.Duration.Companion.milliseconds

class DynamoDbConnectorTest {

    @BeforeEach
    fun setUp() {
        MockKAnnotations.init(this)
        clearAllMocks()
    }

    @AfterEach
    fun tearDown() {
        clearAllMocks()
    }

    @Test
    fun `getClient uses pipeline id and falls back to unknown when pipeline is absent`() {
        // Arrange
        val pipelineClient = mockk<DynamoDbClient>()
        val unknownClient = mockk<DynamoDbClient>()
        val clientFactory = mockk<DynamoDbClientFactory>()
        val pipeline = testPipeline("pipeline-1")

        every { clientFactory.getClient("pipeline-1") } returns pipelineClient
        every { clientFactory.getClient("unknown") } returns unknownClient

        val connector = DynamoDbConnector(dynamoDbClientFactory = clientFactory)

        // Act
        val resultForPipeline = connector.getClient(UnitOfWork(pipeline = pipeline))
        val resultForMissingPipeline = connector.getClient(UnitOfWork())

        // Assert
        resultForPipeline shouldBe pipelineClient
        resultForMissingPipeline shouldBe unknownClient
    }

    @Test
    fun `put delegates to client, returns response, and debugs successful response`() = runTest {
        // Arrange
        val client = mockk<DynamoDbClient>()
        val clientFactory = mockk<DynamoDbClientFactory>()
        val debugValues = mutableListOf<Any?>()
        val request = PutItemRequest {
            tableName = "events"
            item = mapOf("pk" to AttributeValue.S("event-1"))
        }
        val response = PutItemResponse { }

        every { clientFactory.getClient("unknown") } returns client
        coEvery { client.putItem(request) } returns response

        val connector = DynamoDbConnector(
            debug = { debugValues += it },
            dynamoDbClientFactory = clientFactory,
        )

        // Act
        val result = connector.put(request, UnitOfWork())

        // Assert
        result shouldBe response
        debugValues shouldContainExactly listOf(response)
        coVerify(exactly = 1) { client.putItem(request) }
    }

    @Test
    fun `put debugs and rethrows client exception`() = runTest {
        // Arrange
        val client = mockk<DynamoDbClient>()
        val clientFactory = mockk<DynamoDbClientFactory>()
        val debugValues = mutableListOf<Any?>()
        val request = PutItemRequest { tableName = "events" }
        val exception = IllegalStateException("dynamodb failed")

        every { clientFactory.getClient("unknown") } returns client
        coEvery { client.putItem(request) } throws exception

        val connector = DynamoDbConnector(
            debug = { debugValues += it },
            dynamoDbClientFactory = clientFactory,
        )

        // Act
        val thrown = shouldThrow<IllegalStateException> {
            connector.put(request, UnitOfWork())
        }

        // Assert
        thrown shouldBe exception
        debugValues shouldContainExactly listOf(exception)
        coVerify(exactly = 1) { client.putItem(request) }
    }

    @Test
    fun `update returns response on success and handles conditional failures based on configuration`() = runTest {
        // Arrange
        val successfulClient = mockk<DynamoDbClient>()
        val swallowingClient = mockk<DynamoDbClient>()
        val throwingClient = mockk<DynamoDbClient>()
        val successfulFactory = mockk<DynamoDbClientFactory>()
        val swallowingFactory = mockk<DynamoDbClientFactory>()
        val throwingFactory = mockk<DynamoDbClientFactory>()
        val request = UpdateItemRequest { tableName = "events" }
        val response = UpdateItemResponse { }
        val conditionalFailure = ConditionalCheckFailedException {
            message = "condition failed"
        }

        every { successfulFactory.getClient("unknown") } returns successfulClient
        every { swallowingFactory.getClient("unknown") } returns swallowingClient
        every { throwingFactory.getClient("unknown") } returns throwingClient

        coEvery { successfulClient.updateItem(request) } returns response
        coEvery { swallowingClient.updateItem(request) } throws conditionalFailure
        coEvery { throwingClient.updateItem(request) } throws conditionalFailure

        val successfulConnector = DynamoDbConnector(dynamoDbClientFactory = successfulFactory)
        val swallowingConnector = DynamoDbConnector(dynamoDbClientFactory = swallowingFactory)
        val throwingConnector = DynamoDbConnector(
            throwConditionFailure = true,
            dynamoDbClientFactory = throwingFactory,
        )

        // Act
        val successfulResult = successfulConnector.update(request, UnitOfWork())
        val swallowedResult = swallowingConnector.update(request, UnitOfWork())
        val thrown = shouldThrow<ConditionalCheckFailedException> {
            throwingConnector.update(request, UnitOfWork())
        }

        // Assert
        successfulResult shouldBe response
        swallowedResult shouldBe null
        thrown shouldBe conditionalFailure

        coVerify(exactly = 1) { successfulClient.updateItem(request) }
        coVerify(exactly = 1) { swallowingClient.updateItem(request) }
        coVerify(exactly = 1) { throwingClient.updateItem(request) }
    }

    @Test
    fun `queryAll follows last evaluated keys and combines all items into one response`() = runTest {
        // Arrange
        val client = mockk<DynamoDbClient>()
        val clientFactory = mockk<DynamoDbClientFactory>()
        val firstCursor = mapOf("pk" to AttributeValue.S("cursor-1"))
        val firstItem = mapOf("pk" to AttributeValue.S("event-1"))
        val secondItem = mapOf("pk" to AttributeValue.S("event-2"))

        val request = QueryRequest {
            tableName = "events"
            keyConditionExpression = "pk = :pk"
            expressionAttributeValues = mapOf(":pk" to AttributeValue.S("partition-1"))
        }

        every { clientFactory.getClient("unknown") } returns client

        coEvery {
            client.query(match { it.exclusiveStartKey == null })
        } returns QueryResponse {
            items = listOf(firstItem)
            count = 1
            lastEvaluatedKey = firstCursor
        }

        coEvery {
            client.query(match { it.exclusiveStartKey == firstCursor })
        } returns QueryResponse {
            items = listOf(secondItem)
            count = 1
            lastEvaluatedKey = emptyMap()
        }

        val connector = DynamoDbConnector(dynamoDbClientFactory = clientFactory)

        // Act
        val result = connector.queryAll(request, UnitOfWork())

        // Assert
        result.items shouldContainExactly listOf(firstItem, secondItem)
        result.count shouldBe 2
        result.lastEvaluatedKey shouldBe null

        coVerify(exactly = 1) { client.query(match { it.exclusiveStartKey == null }) }
        coVerify(exactly = 1) { client.query(match { it.exclusiveStartKey == firstCursor }) }
    }

    @Test
    fun `queryAll starts from request cursor and stops when limit is reached`() = runTest {
        // Arrange
        val client = mockk<DynamoDbClient>()
        val clientFactory = mockk<DynamoDbClientFactory>()
        val initialCursor = mapOf("pk" to AttributeValue.S("initial-cursor"))
        val nextCursor = mapOf("pk" to AttributeValue.S("next-cursor"))
        val item = mapOf("pk" to AttributeValue.S("event-1"))

        val request = QueryRequest {
            tableName = "events"
            limit = 1
            exclusiveStartKey = initialCursor
        }

        every { clientFactory.getClient("unknown") } returns client

        coEvery {
            client.query(match { it.exclusiveStartKey == initialCursor && it.limit == 1 })
        } returns QueryResponse {
            items = listOf(item)
            count = 1
            lastEvaluatedKey = nextCursor
        }

        val connector = DynamoDbConnector(dynamoDbClientFactory = clientFactory)

        // Act
        val result = connector.queryAll(request, UnitOfWork())

        // Assert
        result.items shouldContainExactly listOf(item)
        result.count shouldBe 1
        result.lastEvaluatedKey shouldBe null

        coVerify(exactly = 1) {
            client.query(match { it.exclusiveStartKey == initialCursor && it.limit == 1 })
        }
        coVerify(exactly = 0) {
            client.query(match { it.exclusiveStartKey == nextCursor })
        }
    }

    @Test
    fun `batchGetItem retries unprocessed keys and combines responses`() = runTest {
        // Arrange
        val client = mockk<DynamoDbClient>()
        val clientFactory = mockk<DynamoDbClientFactory>()
        val firstKey = mapOf("pk" to AttributeValue.S("event-1"))
        val secondKey = mapOf("pk" to AttributeValue.S("event-2"))
        val firstItem = mapOf("pk" to AttributeValue.S("event-1"), "value" to AttributeValue.S("first"))
        val secondItem = mapOf("pk" to AttributeValue.S("event-2"), "value" to AttributeValue.S("second"))

        val originalRequest = BatchGetItemRequest {
            requestItems = mapOf(
                "events" to KeysAndAttributes {
                    keys = listOf(firstKey, secondKey)
                }
            )
        }

        val unprocessedRequestItems = mapOf(
            "events" to KeysAndAttributes {
                keys = listOf(secondKey)
            }
        )

        every { clientFactory.getClient("unknown") } returns client

        coEvery {
            client.batchGetItem(originalRequest)
        } returns BatchGetItemResponse {
            responses = mapOf("events" to listOf(firstItem))
            unprocessedKeys = unprocessedRequestItems
        }

        coEvery {
            client.batchGetItem(match { it.requestItems == unprocessedRequestItems })
        } returns BatchGetItemResponse {
            responses = mapOf("events" to listOf(secondItem))
            unprocessedKeys = emptyMap()
        }

        val connector = DynamoDbConnector(
            dynamoDbClientFactory = clientFactory,
            retryConfig = RetryConfig(maxRetries = 1, retryWait = 1.milliseconds),
        )

        // Act
        val result = connector.batchGetItem(originalRequest, UnitOfWork())

        // Assert
        result.responses?.shouldContainExactly(
            mapOf(
                "events" to listOf(firstItem, secondItem)
            )
        )
        result.unprocessedKeys shouldBe emptyMap()

        coVerify(exactly = 1) { client.batchGetItem(originalRequest) }
        coVerify(exactly = 1) {
            client.batchGetItem(match { it.requestItems == unprocessedRequestItems })
        }
    }

    private fun testPipeline(id: String): Pipeline {
        return object : Pipeline(id) {
            override fun connect(
                fm: FaultManager,
                fromFlow: Flow<UnitOfWork>,
            ): Flow<UnitOfWork> = fromFlow
        }
    }
}