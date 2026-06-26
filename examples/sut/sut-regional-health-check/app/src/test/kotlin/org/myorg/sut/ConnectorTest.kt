package org.myorg.sut

import aws.sdk.kotlin.services.dynamodb.DynamoDbClient
import aws.sdk.kotlin.services.dynamodb.model.*
import io.github.huherto.awsLambdaStream.sinks.DynamoDbUpdateValue
import io.kotest.matchers.collections.shouldContainExactly
import io.kotest.matchers.maps.shouldContainExactly
import io.kotest.matchers.shouldBe
import io.kotest.matchers.string.shouldContain
import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.mockk
import io.mockk.slot
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test

class ConnectorTest {

    @Test
    fun `get queries DynamoDB with expected request and returns items`() = runTest {
        // Arrange
        val tableName = "health-check-table"
        val id = "eu-west-1"
        val debugMessages = mutableListOf<String>()
        val dynamoDbClient = mockk<DynamoDbClient>()

        val expectedItems = listOf(
            mapOf(
                "pk" to AttributeValue.S(id),
                "sk" to AttributeValue.S("123"),
                "status" to AttributeValue.S("COMPLETED"),
            ),
            mapOf(
                "pk" to AttributeValue.S(id),
                "sk" to AttributeValue.S("122"),
                "status" to AttributeValue.S("STARTED"),
            ),
        )

        val requestSlot = slot<QueryRequest>()

        coEvery {
            dynamoDbClient.query(capture(requestSlot))
        } returns QueryResponse {
            items = expectedItems
        }

        val connector = Connector(
            debug = debugMessages::add,
            tableName = tableName,
            db = dynamoDbClient,
        )

        // Act
        val result = connector.get(id)

        // Assert
        result.shouldContainExactly(expectedItems)

        val request = requestSlot.captured
        request.tableName shouldBe tableName
        request.limit shouldBe 3
        request.scanIndexForward shouldBe false
        request.keyConditionExpression shouldBe "#pk = :pk"
        request.expressionAttributeNames?.shouldContainExactly(
            mapOf(
                "#pk" to "pk",
            )
        )
        request.expressionAttributeValues?.shouldContainExactly(
            mapOf(
                ":pk" to AttributeValue.S(id),
            )
        )
        request.consistentRead shouldBe true

        debugMessages.size shouldBe 1

        coVerify(exactly = 1) {
            dynamoDbClient.query(any())
        }
    }

    @Test
    fun `get returns empty list when DynamoDB query returns no items`() = runTest {
        // Arrange
        val dynamoDbClient = mockk<DynamoDbClient>()

        coEvery {
            dynamoDbClient.query(any())
        } returns QueryResponse {
            items = null
        }

        val connector = Connector(
            debug = {},
            tableName = "health-check-table",
            db = dynamoDbClient,
        )

        // Act
        val result = connector.get("eu-west-1")

        // Assert
        result.shouldContainExactly()

        coVerify(exactly = 1) {
            dynamoDbClient.query(any())
        }
    }

    @Test
    fun `get logs and rethrows DynamoDB query failure`() = runTest {
        // Arrange
        val debugMessages = mutableListOf<String>()
        val dynamoDbClient = mockk<DynamoDbClient>()
        val failure = RuntimeException("query failed")

        coEvery {
            dynamoDbClient.query(any())
        } throws failure

        val connector = Connector(
            debug = debugMessages::add,
            tableName = "health-check-table",
            db = dynamoDbClient,
        )

        // Act
        val thrown = runCatching {
            connector.get("eu-west-1")
        }.exceptionOrNull()

        // Assert
        thrown shouldBe failure
        debugMessages.size shouldBe 1
        debugMessages.single() shouldContain "query failed"

        coVerify(exactly = 1) {
            dynamoDbClient.query(any())
        }
    }

    @Test
    fun `update sends expected UpdateItemRequest and returns response`() = runTest {
        // Arrange
        val tableName = "health-check-table"
        val debugMessages = mutableListOf<String>()
        val dynamoDbClient = mockk<DynamoDbClient>()

        val key = mapOf(
            "pk" to AttributeValue.S("eu-west-1"),
            "sk" to AttributeValue.S("123"),
        )

        val inputParams = mapOf(
            "status" to DynamoDbUpdateValue.DbSet(AttributeValue.S("STARTED")),
            "latched" to DynamoDbUpdateValue.DbRemove,
        )

        val response = UpdateItemResponse {
            attributes = mapOf(
                "status" to AttributeValue.S("STARTED"),
            )
        }

        val requestSlot = slot<UpdateItemRequest>()

        coEvery {
            dynamoDbClient.updateItem(capture(requestSlot))
        } returns response

        val connector = Connector(
            debug = debugMessages::add,
            tableName = tableName,
            db = dynamoDbClient,
        )

        // Act
        val result = connector.update(
            key = key,
            inputParams = inputParams,
        )

        // Assert
        result shouldBe response

        val request = requestSlot.captured
        request.tableName shouldBe tableName
        request.key?.shouldContainExactly(key)
        request.returnValues shouldBe ReturnValue.AllNew
        request.updateExpression shouldContain "SET"
        request.updateExpression shouldContain "REMOVE"

        debugMessages.size shouldBe 1

        coVerify(exactly = 1) {
            dynamoDbClient.updateItem(any())
        }
    }

    @Test
    fun `update logs and rethrows DynamoDB update failure`() = runTest {
        // Arrange
        val debugMessages = mutableListOf<String>()
        val dynamoDbClient = mockk<DynamoDbClient>()
        val failure = RuntimeException("update failed")

        coEvery {
            dynamoDbClient.updateItem(any())
        } throws failure

        val connector = Connector(
            debug = debugMessages::add,
            tableName = "health-check-table",
            db = dynamoDbClient,
        )

        // Act
        val thrown = runCatching {
            connector.update(
                key = mapOf(
                    "pk" to AttributeValue.S("eu-west-1"),
                    "sk" to AttributeValue.S("123"),
                ),
                inputParams = mapOf(
                    "status" to DynamoDbUpdateValue.DbSet(AttributeValue.S("STARTED")),
                ),
            )
        }.exceptionOrNull()

        // Assert
        thrown shouldBe failure
        debugMessages.size shouldBe 1
        debugMessages.single() shouldContain "update failed"

        coVerify(exactly = 1) {
            dynamoDbClient.updateItem(any())
        }
    }
}