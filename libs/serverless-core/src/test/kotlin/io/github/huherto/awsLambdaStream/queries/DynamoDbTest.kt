package io.github.huherto.awsLambdaStream.queries

import aws.sdk.kotlin.services.dynamodb.DynamoDbClient
import aws.sdk.kotlin.services.dynamodb.model.*
import io.github.huherto.awsLambdaStream.Event
import io.github.huherto.awsLambdaStream.UnitOfWork
import io.github.huherto.awsLambdaStream.from.RecordImage
import io.github.huherto.awsLambdaStream.from.RecordPair
import io.kotest.matchers.nulls.shouldBeNull
import io.kotest.matchers.nulls.shouldNotBeNull
import io.kotest.matchers.shouldBe
import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.every
import io.mockk.mockk
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import com.amazonaws.services.lambda.runtime.events.models.dynamodb.AttributeValue as EventAV

class DynamodbQueriesTest {

    @BeforeEach
    fun setup() {
        // Clear global cache before each test to ensure isolation
        memoryCache.clear()
    }

    @Test
    fun `should generate valid query requests based on rules`() {
        // Arrange
        val mockEvent = mockk<Event>(relaxed = true) {
            every { partitionKey } returns "test-pk"
        }
        val uow = UnitOfWork(event = mockEvent)

        val defaultRule = Rule()
        val customPkRule = Rule(pkFn = "customPk")
        val indexRule = Rule(indexNm = "my-index", indexFn = "customIndexPk")

        // Act & Assert - Default PK Query
        toPkQueryRequest(uow, defaultRule).let { req ->
            req.keyConditionExpression shouldBe "#pk = :pk"
            req.expressionAttributeNames shouldBe mapOf("#pk" to "pk")
            (req.expressionAttributeValues?.get(":pk") as? AttributeValue.S)?.value shouldBe "test-pk"
            req.consistentRead shouldBe true
        }

        // Act & Assert - Custom PK Query
        toPkQueryRequest(uow, customPkRule).let { req ->
            req.expressionAttributeNames shouldBe mapOf("#pk" to "customPk")
        }

        // Act & Assert - Index Query
        toIndexQueryRequest(uow, indexRule).let { req ->
            req.indexName shouldBe "my-index"
            req.keyConditionExpression shouldBe "#pk = :pk"
            req.expressionAttributeNames shouldBe mapOf("#pk" to "customIndexPk")
            (req.expressionAttributeValues?.get(":pk") as? AttributeValue.S)?.value shouldBe "test-pk"
            req.consistentRead shouldBe false
        }
    }

    @Test
    fun `should generate valid get requests or empty if fields are missing`() {
        // Arrange - Setup Event with simulated record map bypassing type erasure
        val mapWithString = mapOf("fk1" to EventAV("discriminator|my-pk"))

        @Suppress("UNCHECKED_CAST")
        val recordImage = RecordImage(mapWithString as Map<String, EventAV?>)
        val rawPair = RecordPair(new = recordImage, old = null)

        val mockEventWithData = mockk<Event>(relaxed = true) {
            every { raw } returns rawPair
        }
        val uowWithData = UnitOfWork(event = mockEventWithData)
        val rule = Rule(fks = listOf("fk1", "missing_fk"), tableName = "my-table")

        // Act - Request with Data
        val request = toGetRequest(uowWithData, rule)

        // Assert - Request with Data
        request.requestItems.shouldNotBeNull()
        val keysAndAttrs = request.requestItems!!["my-table"]
        keysAndAttrs.shouldNotBeNull()
        keysAndAttrs.keys?.size shouldBe 1

        val key = keysAndAttrs.keys!![0]
        (key["sk"] as? AttributeValue.S)?.value shouldBe "discriminator"
        (key["pk"] as? AttributeValue.S)?.value shouldBe "my-pk"

        // Arrange - Empty Request Event
        val emptyUow = UnitOfWork(event = null)

        // Act - Empty Request
        val emptyRequest = toGetRequest(emptyUow, rule)

        // Assert - Empty Request Fallback
        emptyRequest.requestItems.shouldBeNull()
    }

    @Test
    fun `should execute batch and query flows with caching and decryption`() = runBlocking {
        // Arrange - Common setup
        val dynamoDbClient = mockk<DynamoDbClient>()
        val options = DynamoDbOptions(decrypt = { item ->
            item + ("decrypted" to AttributeValue.S("true"))
        })

        // Arrange - Query Mocks
        val queryReq = QueryRequest { tableName = "test-table" }
        val queryUow = UnitOfWork(queryRequest = queryReq)
        coEvery { dynamoDbClient.query(any()) } returns QueryResponse {
            this.items = listOf(mapOf("id" to AttributeValue.S("q1")))
        }

        // Arrange - Batch Mocks
        val batchReq = BatchGetItemRequest {
            requestItems = mapOf("table" to KeysAndAttributes { keys = listOf(mapOf("pk" to AttributeValue.S("1"))) })
        }

        val batchUow = UnitOfWork(batchGetRequest = batchReq)

        coEvery { dynamoDbClient.batchGetItem(any()) } returns BatchGetItemResponse {
            responses = mapOf("table" to listOf(mapOf("id" to AttributeValue.S("b1"))))
        }

        // Act - Execute Flows
        val queryResults = flowOf(queryUow).queryAllDynamoDB(dynamoDbClient, options).toList()
        val batchResults = flowOf(batchUow).batchGetDynamoDB(dynamoDbClient, options).toList()

        // Assert - Decryption Output
        queryResults.size shouldBe 1
        val queryItem = queryResults[0].queryResponse?.items?.firstOrNull()
        (queryItem?.get("id") as? AttributeValue.S)?.value shouldBe "q1"
        (queryItem?.get("decrypted") as? AttributeValue.S)?.value shouldBe "true"

        batchResults.size shouldBe 1

        // Act & Assert - Caching (Second call should hit memoryCache, verifying no subsequent AWS calls)
        flowOf(queryUow).queryAllDynamoDB(dynamoDbClient, options).toList()
        coVerify(exactly = 1) { dynamoDbClient.query(any()) }

        flowOf(batchUow).batchGetDynamoDB(dynamoDbClient, options).toList()
        coVerify(exactly = 1) { dynamoDbClient.batchGetItem(any()) }
    }

    @Test
    fun `should split scan and query results handling pagination properly`() = runBlocking {
        // Arrange
        val dynamoDbClient = mockk<DynamoDbClient>()

        // Arrange - Scan Split (with explicit limit pagination requirement)
        val scanReq = ScanRequest { limit = 2 }
        val scanUow = UnitOfWork(scanRequest = scanReq)

        coEvery { dynamoDbClient.scan(match { it.exclusiveStartKey == null }) } returns ScanResponse {
            items = listOf(mapOf("id" to AttributeValue.S("s1")))
            lastEvaluatedKey = mapOf("id" to AttributeValue.S("s1"))
        }
        coEvery { dynamoDbClient.scan(match { it.exclusiveStartKey != null }) } returns ScanResponse {
            items = listOf(mapOf("id" to AttributeValue.S("s2")))
            lastEvaluatedKey = null
        }

        // Arrange - Query Split
        val queryReq = QueryRequest { limit = 1 }
        val queryUow = UnitOfWork(queryRequest = queryReq)

        coEvery { dynamoDbClient.query(any()) } returns QueryResponse {
            items = listOf(mapOf("id" to AttributeValue.S("qs1")), mapOf("id" to AttributeValue.S("qs2")))
            lastEvaluatedKey = null // No pagination next step
        }

        // Act
        val scanResults = flowOf(scanUow).scanSplitDynamoDB(dynamoDbClient).toList()
        val queryResults = flowOf(queryUow).querySplitDynamoDB(dynamoDbClient).toList()

        // Assert - Both should return two items but handle underlying request calls differently based on mock
        scanResults.size shouldBe 2 // split properly between two API calls
        coVerify(exactly = 2) { dynamoDbClient.scan(any()) }

        queryResults.size shouldBe 2 // split properly from single API call yielding 2 events
        coVerify(exactly = 1) { dynamoDbClient.query(any()) }
    }
}