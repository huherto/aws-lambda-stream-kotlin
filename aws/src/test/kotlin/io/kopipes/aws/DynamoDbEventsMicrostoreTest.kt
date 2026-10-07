package io.kopipes.aws

import aws.sdk.kotlin.services.dynamodb.DynamoDbClient
import aws.sdk.kotlin.services.dynamodb.model.AttributeValue
import aws.sdk.kotlin.services.dynamodb.model.PutItemResponse
import aws.sdk.kotlin.services.dynamodb.model.QueryResponse
import io.kopipes.aws.connectors.DynamoDbClientFactory
import io.kopipes.aws.extensions.*
import io.kopipes.aws.sinks.DynamoDbEventsMicrostore
import io.kopipes.core.*
import io.kopipes.core.faults.FaultManager
import io.kopipes.core.sinks.EventPublisher
import io.kopipes.core.sinks.EventsMicrostore
import io.kopipes.core.sinks.withQueryParams
import io.kopipes.core.sinks.withSaveOptions
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.nulls.shouldBeNull
import io.kotest.matchers.nulls.shouldNotBeNull
import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.shouldBeTypeOf
import io.mockk.*
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.async
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test
import kotlin.time.Duration.Companion.milliseconds

class DynamoDbEventsMicrostoreTest {

    private val dynamoDbClient = mockk<DynamoDbClient>()
    private val faultManager = mockk<FaultManager>(relaxed = true)
    private val dynamoDbClientFactory by lazy {
        val factory = spyk<DynamoDbClientFactory>()
        every { factory.getClient(any()) } returns dynamoDbClient
        factory
    }

    private val eventMicrostore = DynamoDbEventsMicrostore(dynamoDbClientFactory, faultManager)

    @Test
    fun `putRequest should correctly populate PutItemRequest based on UnitOfWork, Event, and EnvironmentConfig`() {
        // Arrange
        val eventId = "test-event-id"
        val eventTimestamp = 1672531200000L
        val eventEncoded = "{\"data\":\"encoded-event\"}"
        val awsRegion = "eu-west-1"
        val expectedTableName = "events"

        val mockEvent = object : Event {
            override val id = eventId
            override val timestamp = eventTimestamp
            override val partitionKey = null
            override val tags = null
            override val raw = null
            override val eem = null
            override val triggers = null
            override fun eventType() = "TEST_EVENT"
            override fun toString() = eventEncoded
            override fun copyEvent(
                id: String?,
                timestamp: Long?,
                partitionKey: String?,
                tags: Map<String, String>?,
                raw: RawRecord?,
                eem: EnvelopeEncryptionMetadata?,
                triggers: List<EventReference>?
            ): Event {
                TODO("Not needed for this test")
            }
        }

        val savedOptions = EventsMicrostore.SaveOptions(
            pk = eventId,
            sk = "EVENT",
            discriminator = "EVENT",
            timeStamp = eventTimestamp,
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
        ).withSaveOptions(savedOptions)

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
    fun `toQueryRequest should set queryRequest with consistentRead when correlation is true and pk is provided`() {
        // Arrange
        val uow = UnitOfWork().withQueryParams(EventsMicrostore.QueryParams(
            pk = "test-pk",
            correlation = true
        ))

        // Act
        val result = eventMicrostore.toQueryRequest(uow)

        // Assert
        val request = result.queryRequest.shouldNotBeNull()
        request.tableName shouldBe "events"
        request.indexName.shouldBeNull()
        request.keyConditionExpression shouldBe "#pk = :pk"
        request.expressionAttributeNames shouldBe mapOf("#pk" to "pk")
        
        val pkValue = request.expressionAttributeValues?.get(":pk")
        pkValue.shouldNotBeNull()
        pkValue.shouldBeTypeOf<AttributeValue.S>().value shouldBe "test-pk"
        request.consistentRead shouldBe true
    }

    @Test
    fun `toQueryRequest should set queryRequest on DataIndex without consistentRead when correlation is false and data is provided`() {
        // Arrange
        val uow = UnitOfWork().withQueryParams(EventsMicrostore.QueryParams(
            data = "test-data",
            correlation = false
        ))

        // Act
        val result = eventMicrostore.toQueryRequest(uow)

        // Assert
        val request = result.queryRequest.shouldNotBeNull()
        request.tableName shouldBe "events"
        request.indexName shouldBe "DataIndex"
        request.keyConditionExpression shouldBe "#data = :data"
        request.expressionAttributeNames shouldBe mapOf("#data" to "data")

        val dataValue = request.expressionAttributeValues?.get(":data")
        dataValue.shouldNotBeNull()
        dataValue.shouldBeTypeOf<AttributeValue.S>().value shouldBe "test-data"
        request.consistentRead shouldBe null
    }

    @Test
    fun `toQueryRequest should use custom index name when specified in queryParams`() {
        // Arrange
        val uow = UnitOfWork().withQueryParams(EventsMicrostore.QueryParams(
            data = "test-data",
            correlation = false,
            index = "CustomGsiIndex"
        ))

        // Act
        val result = eventMicrostore.toQueryRequest(uow)

        // Assert
        val request = result.queryRequest.shouldNotBeNull()
        request.indexName shouldBe "CustomGsiIndex"
        request.consistentRead shouldBe null
    }

    @Test
    fun `toQueryRequest should return original uow when queryParams is null`() {
        // Arrange
        val uow = UnitOfWork()

        // Act
        val result = eventMicrostore.toQueryRequest(uow)

        // Assert
        result.queryRequest.shouldBeNull()
        result shouldBe uow
    }

    @Test
    fun `toQueryRequest should return original uow when correlation is true but pk is missing or empty`() {
        // Arrange
        val uowNullPk = UnitOfWork().withQueryParams(EventsMicrostore.QueryParams(
            pk = null,
            correlation = true
        ))
        val uowEmptyPk = UnitOfWork().withQueryParams(EventsMicrostore.QueryParams(
            pk = "",
            correlation = true
        ))

        // Act & Assert
        eventMicrostore.toQueryRequest(uowNullPk).queryRequest.shouldBeNull()
        eventMicrostore.toQueryRequest(uowEmptyPk).queryRequest.shouldBeNull()
    }

    @Test
    fun `toQueryRequest should return original uow when correlation is false but data is missing or empty`() {
        // Arrange
        val uowNullData = UnitOfWork().withQueryParams(EventsMicrostore.QueryParams(
            data = null,
            correlation = false
        ))
        val uowEmptyData = UnitOfWork().withQueryParams(EventsMicrostore.QueryParams(
            data = "",
            correlation = false
        ))

        // Act & Assert
        eventMicrostore.toQueryRequest(uowNullData).queryRequest.shouldBeNull()
        eventMicrostore.toQueryRequest(uowEmptyData).queryRequest.shouldBeNull()
    }

    @Test
    fun `unmarshall should return parsed JsonEvent for valid json string`() {
        // Arrange
        val jsonString = "{\"id\":\"evt-123\", \"type\":\"TEST_EVENT\"}"
        
        // Act
        val result = eventMicrostore.unmarshall(jsonString)

        // Assert
        val parsed = result.shouldNotBeNull()
        parsed.id shouldBe "evt-123"
        parsed.eventType() shouldBe "TEST_EVENT"
    }

    @Test
    fun `unmarshall should record fault and return null for invalid json string`() {
        // Arrange
        val invalidJson = "invalid-json"
        val uow = UnitOfWork(key = "test-uow")
        
        // Act
        val result = eventMicrostore.unmarshall(invalidJson, uow)

        // Assert
        result.shouldBeNull()
        verify(exactly = 1) {
            faultManager.redirectFailure(match { it.uow == uow && it.cause is Exception })
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

        val uow = UnitOfWork().withQueryResponse(QueryResponse {
            items = itemsList
        })

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

    @Test
    fun `toCorrelated should extract valid events and record faults for corrupted items`() {
        // Arrange
        val validJson1 = "{\"id\":\"evt-1\", \"type\":\"TYPE_1\"}"
        val invalidJson = "invalid-json"
        val validJson2 = "{\"id\":\"evt-2\", \"type\":\"TYPE_2\"}"

        val itemsList = listOf(
            mapOf("event" to AttributeValue.S(validJson1)),
            mapOf("event" to AttributeValue.S(invalidJson)),
            mapOf("event" to AttributeValue.S(validJson2)),
        )

        val uow = UnitOfWork(key = "corr-test").withQueryResponse(QueryResponse {
            items = itemsList
        })

        // Act
        val result = eventMicrostore.toCorrelated(uow)

        // Assert
        val correlated = result.correlated.shouldNotBeNull()
        correlated shouldHaveSize 2
        correlated[0].id shouldBe "evt-1"
        correlated[1].id shouldBe "evt-2"

        verify(exactly = 1) {
            faultManager.redirectFailure(match { it.uow == uow })
        }
    }

    @Test
    fun `save should execute PutItem on DynamoDbClient for each unit of work and attach PutItemResponse`() = runTest {
        // Arrange
        val client = mockk<DynamoDbClient>()
        val factory = mockk<DynamoDbClientFactory>()
        every { factory.getClient(any()) } returns client

        val eventPublisher = mockk<EventPublisher>(relaxed = true)
        val fm = FaultManager(eventPublisher, skipErrorLogging = true)
        val microstore = DynamoDbEventsMicrostore(
            dynamoDbClientFactory = factory,
            faultManager = fm,
            parallel = 2
        )

        val uow1 = UnitOfWork(key = "k1").withSaveOptions(EventsMicrostore.SaveOptions(
            pk = "pk1",
            sk = "EVENT",
            discriminator = "EVENT",
            timeStamp = 123456L,
            expire = false,
            suffix = ""
        ))
        val uow2 = UnitOfWork(key = "k2").withSaveOptions(EventsMicrostore.SaveOptions(
            pk = "pk2",
            sk = "EVENT",
            discriminator = "EVENT",
            timeStamp = 123456L,
            expire = false,
            suffix = ""
        ))

        val expectedResponse1 = PutItemResponse {}
        val expectedResponse2 = PutItemResponse {}

        coEvery { client.putItem(match { it.item?.get("pk") == AttributeValue.S("pk1") }) } returns expectedResponse1
        coEvery { client.putItem(match { it.item?.get("pk") == AttributeValue.S("pk2") }) } returns expectedResponse2

        // Act
        val results = microstore.save(flowOf(uow1, uow2)).toList()

        // Assert
        results shouldHaveSize 2
        val res1 = results.first { it.key == "k1" }
        val res2 = results.first { it.key == "k2" }

        res1.putResponse shouldBe expectedResponse1
        res2.putResponse shouldBe expectedResponse2

        coVerify(exactly = 2) { client.putItem(any()) }
    }

    @Test
    fun `save should route exceptions during putItem to FaultManager`() = runTest {
        // Arrange
        val client = mockk<DynamoDbClient>()
        val factory = mockk<DynamoDbClientFactory>()
        every { factory.getClient(any()) } returns client

        val eventPublisher = mockk<EventPublisher>(relaxed = true)
        val fm = FaultManager(eventPublisher, skipErrorLogging = true)
        val microstore = DynamoDbEventsMicrostore(
            dynamoDbClientFactory = factory,
            faultManager = fm,
            parallel = 1
        )

        val uow = UnitOfWork(key = "failing-put").withSaveOptions(EventsMicrostore.SaveOptions(
            pk = "pk-fail",
            sk = "EVENT",
            discriminator = "EVENT",
            timeStamp = 123456L,
            expire = false,
            suffix = ""
        ))
        coEvery { client.putItem(any()) } throws RuntimeException("DynamoDB error")

        // Act
        val results = microstore.save(flowOf(uow)).toList()

        // Assert
        results shouldHaveSize 0
        fm.getFaults() shouldHaveSize 1
    }

    @Test
    fun `save should execute concurrently with configured parallelism`() = runTest {
        // Arrange
        val client = mockk<DynamoDbClient>()
        val factory = mockk<DynamoDbClientFactory>()
        every { factory.getClient(any()) } returns client

        val eventPublisher = mockk<EventPublisher>(relaxed = true)
        val fm = FaultManager(eventPublisher, skipErrorLogging = true)
        val microstore = DynamoDbEventsMicrostore(
            dynamoDbClientFactory = factory,
            faultManager = fm,
            parallel = 2
        )

        val uowList = (1..3).map {
            UnitOfWork(key = "key-$it").withSaveOptions(EventsMicrostore.SaveOptions(
                pk = "pk-$it",
                sk = "EVENT",
                discriminator = "EVENT",
                timeStamp = 123456L,
                expire = false,
                suffix = ""
            ))
        }

        val firstTwoStarted = CompletableDeferred<Unit>()
        var activeCalls = 0
        var maxActiveCalls = 0

        coEvery { client.putItem(any()) } coAnswers {
            activeCalls += 1
            maxActiveCalls = maxOf(maxActiveCalls, activeCalls)
            if (activeCalls == 2) {
                firstTwoStarted.complete(Unit)
            }
            delay(100.milliseconds)
            activeCalls -= 1
            PutItemResponse {}
        }

        // Act
        val collection = async {
            microstore.save(uowList.asFlow()).toList()
        }

        firstTwoStarted.await()
        val results = collection.await()

        // Assert
        results shouldHaveSize 3
        maxActiveCalls shouldBe 2
    }

    @Test
    fun `queryByPk should execute Query on DynamoDbClient and populate correlated events`() = runTest {
        // Arrange
        val client = mockk<DynamoDbClient>()
        val factory = mockk<DynamoDbClientFactory>()
        every { factory.getClient(any()) } returns client

        val eventPublisher = mockk<EventPublisher>(relaxed = true)
        val fm = FaultManager(eventPublisher, skipErrorLogging = true)
        val microstore = DynamoDbEventsMicrostore(
            dynamoDbClientFactory = factory,
            faultManager = fm,
            parallel = 2
        )

        val uow = UnitOfWork(key = "q1").withQueryParams(EventsMicrostore.QueryParams(
            pk = "test-pk",
            correlation = true
        ))

        val eventJson = "{\"id\":\"corr-1\", \"type\":\"CORR_TYPE\"}"
        val queryResponse = QueryResponse {
            items = listOf(mapOf("event" to AttributeValue.S(eventJson)))
            count = 1
        }

        coEvery { client.query(any()) } returns queryResponse

        // Act
        val results = microstore.queryByPk(flowOf(uow)).toList()

        // Assert
        results shouldHaveSize 1
        val result = results.first()
        result.queryResponse shouldBe queryResponse
        val correlated = result.correlated.shouldNotBeNull()
        correlated shouldHaveSize 1
        correlated[0].id shouldBe "corr-1"
        correlated[0].eventType() shouldBe "CORR_TYPE"

        coVerify(exactly = 1) { client.query(any()) }
    }

    @Test
    fun `queryByPk should route exceptions during query to FaultManager`() = runTest {
        // Arrange
        val client = mockk<DynamoDbClient>()
        val factory = mockk<DynamoDbClientFactory>()
        every { factory.getClient(any()) } returns client

        val eventPublisher = mockk<EventPublisher>(relaxed = true)
        val fm = FaultManager(eventPublisher, skipErrorLogging = true)
        val microstore = DynamoDbEventsMicrostore(
            dynamoDbClientFactory = factory,
            faultManager = fm,
            parallel = 1
        )

        val uow = UnitOfWork(key = "failing-query").withQueryParams(EventsMicrostore.QueryParams(
            pk = "test-pk",
            correlation = true
        ))

        coEvery { client.query(any()) } throws RuntimeException("Query failed")

        // Act
        val results = microstore.queryByPk(flowOf(uow)).toList()

        // Assert
        results shouldHaveSize 0
        fm.getFaults() shouldHaveSize 1
    }

    @Test
    fun `queryByPk should execute concurrently with configured parallelism`() = runTest {
        // Arrange
        val client = mockk<DynamoDbClient>()
        val factory = mockk<DynamoDbClientFactory>()
        every { factory.getClient(any()) } returns client

        val eventPublisher = mockk<EventPublisher>(relaxed = true)
        val fm = FaultManager(eventPublisher, skipErrorLogging = true)
        val microstore = DynamoDbEventsMicrostore(
            dynamoDbClientFactory = factory,
            faultManager = fm,
            parallel = 2
        )

        val uowList = (1..3).map {
            UnitOfWork(key = "q-$it").withQueryParams(EventsMicrostore.QueryParams(
                pk = "pk-$it",
                correlation = true
            ))
        }

        val firstTwoStarted = CompletableDeferred<Unit>()
        var activeCalls = 0
        var maxActiveCalls = 0

        coEvery { client.query(any()) } coAnswers {
            activeCalls += 1
            maxActiveCalls = maxOf(maxActiveCalls, activeCalls)
            if (activeCalls == 2) {
                firstTwoStarted.complete(Unit)
            }
            delay(100.milliseconds)
            activeCalls -= 1
            QueryResponse { items = emptyList() }
        }

        // Act
        val collection = async {
            microstore.queryByPk(uowList.asFlow()).toList()
        }

        firstTwoStarted.await()
        val results = collection.await()

        // Assert
        results shouldHaveSize 3
        maxActiveCalls shouldBe 2
    }

    @Test
    fun `queryByPk should paginate and aggregate items across multiple pages when lastEvaluatedKey is present`() = runTest {
        // Arrange
        val client = mockk<DynamoDbClient>()
        val factory = mockk<DynamoDbClientFactory>()
        every { factory.getClient(any()) } returns client

        val eventPublisher = mockk<EventPublisher>(relaxed = true)
        val fm = FaultManager(eventPublisher, skipErrorLogging = true)
        val microstore = DynamoDbEventsMicrostore(
            dynamoDbClientFactory = factory,
            faultManager = fm,
            parallel = 1
        )

        val uow = UnitOfWork(key = "paginated-query").withQueryParams(EventsMicrostore.QueryParams(
            pk = "test-pk",
            correlation = true
        ))

        val event1Json = "{\"id\":\"evt-page-1\", \"type\":\"PAGE_1\"}"
        val event2Json = "{\"id\":\"evt-page-2\", \"type\":\"PAGE_2\"}"

        val page1Key = mapOf("pk" to AttributeValue.S("test-pk"), "sk" to AttributeValue.S("sk-1"))

        val page1Response = QueryResponse {
            items = listOf(mapOf("event" to AttributeValue.S(event1Json)))
            count = 1
            lastEvaluatedKey = page1Key
        }
        val page2Response = QueryResponse {
            items = listOf(mapOf("event" to AttributeValue.S(event2Json)))
            count = 1
            lastEvaluatedKey = null
        }

        coEvery {
            client.query(match { it.exclusiveStartKey == null })
        } returns page1Response

        coEvery {
            client.query(match { it.exclusiveStartKey == page1Key })
        } returns page2Response

        // Act
        val results = microstore.queryByPk(flowOf(uow)).toList()

        // Assert
        results shouldHaveSize 1
        val result = results.first()
        val response = result.queryResponse.shouldNotBeNull()
        response.items?.size shouldBe 2
        response.count shouldBe 2
        response.lastEvaluatedKey shouldBe null

        val correlated = result.correlated.shouldNotBeNull()
        correlated shouldHaveSize 2
        correlated[0].id shouldBe "evt-page-1"
        correlated[0].eventType() shouldBe "PAGE_1"
        correlated[1].id shouldBe "evt-page-2"
        correlated[1].eventType() shouldBe "PAGE_2"

        coVerify(exactly = 1) { client.query(match { it.exclusiveStartKey == null }) }
        coVerify(exactly = 1) { client.query(match { it.exclusiveStartKey == page1Key }) }
    }

    @Test
    fun `queryByPk should stop pagination when limit on query request is reached`() = runTest {
        // Arrange
        val client = mockk<DynamoDbClient>()
        val factory = mockk<DynamoDbClientFactory>()
        every { factory.getClient(any()) } returns client

        val eventPublisher = mockk<EventPublisher>(relaxed = true)
        val fm = FaultManager(eventPublisher, skipErrorLogging = true)
        val microstore = DynamoDbEventsMicrostore(
            dynamoDbClientFactory = factory,
            faultManager = fm,
            parallel = 1
        )

        // Custom microstore override or query request with limit
        val uow = UnitOfWork(key = "limited-query").withQueryRequest(
            aws.sdk.kotlin.services.dynamodb.model.QueryRequest {
                tableName = "events"
                limit = 1
            }
        )

        val event1Json = "{\"id\":\"evt-limit-1\", \"type\":\"LIMIT_1\"}"
        val page1Key = mapOf("pk" to AttributeValue.S("test-pk"), "sk" to AttributeValue.S("sk-1"))

        val page1Response = QueryResponse {
            items = listOf(mapOf("event" to AttributeValue.S(event1Json)))
            count = 1
            lastEvaluatedKey = page1Key
        }

        coEvery {
            client.query(any())
        } returns page1Response

        // Act
        val results = microstore.queryByPk(flowOf(uow)).toList()

        // Assert
        results shouldHaveSize 1
        val result = results.first()
        val response = result.queryResponse.shouldNotBeNull()
        response.items?.size shouldBe 1
        response.count shouldBe 1

        val correlated = result.correlated.shouldNotBeNull()
        correlated shouldHaveSize 1
        correlated[0].id shouldBe "evt-limit-1"

        coVerify(exactly = 1) { client.query(any()) }
    }

    @Test
    fun `queryByPk should continue processing and record fault in faultManager when corrupted correlated event is present`() = runTest {
        // Arrange
        val client = mockk<DynamoDbClient>()
        val factory = mockk<DynamoDbClientFactory>()
        every { factory.getClient(any()) } returns client

        val eventPublisher = mockk<EventPublisher>(relaxed = true)
        val fm = FaultManager(eventPublisher, skipErrorLogging = true)
        val microstore = DynamoDbEventsMicrostore(
            dynamoDbClientFactory = factory,
            faultManager = fm,
            parallel = 1
        )

        val uow = UnitOfWork(key = "corr-uow").withQueryParams(EventsMicrostore.QueryParams(
            pk = "test-pk",
            correlation = true
        ))

        val validEventJson = "{\"id\":\"valid-1\", \"type\":\"VALID_TYPE\"}"
        val corruptedEventJson = "not-a-valid-json"

        val queryResponse = QueryResponse {
            items = listOf(
                mapOf("event" to AttributeValue.S(validEventJson)),
                mapOf("event" to AttributeValue.S(corruptedEventJson))
            )
            count = 2
        }

        coEvery { client.query(any()) } returns queryResponse

        // Act
        val results = microstore.queryByPk(flowOf(uow)).toList()

        // Assert
        results shouldHaveSize 1
        val result = results.first()
        val correlated = result.correlated.shouldNotBeNull()
        correlated shouldHaveSize 1
        correlated[0].id shouldBe "valid-1"
        correlated[0].eventType() shouldBe "VALID_TYPE"

        fm.getFaults() shouldHaveSize 1
    }
}