package io.github.huherto.awsLambdaStream.flavors

import aws.sdk.kotlin.services.dynamodb.DynamoDbClient
import aws.sdk.kotlin.services.dynamodb.model.AttributeValue
import aws.sdk.kotlin.services.dynamodb.model.UpdateItemRequest
import aws.sdk.kotlin.services.dynamodb.model.UpdateItemResponse
import io.github.huherto.awsLambdaStream.*
import io.github.huherto.awsLambdaStream.connectors.DynamoDbClientFactory
import io.github.huherto.awsLambdaStream.connectors.DynamoDbConnector
import io.github.huherto.awsLambdaStream.extensions.updateRequest
import io.github.huherto.awsLambdaStream.extensions.updateResponse
import io.github.huherto.awsLambdaStream.filters.EventFilter
import io.github.huherto.awsLambdaStream.sinks.EventPublisher
import io.kotest.matchers.collections.shouldContainExactly
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.shouldBe
import io.mockk.*
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test

class MaterializePipelineTest {

    @Test
    fun `connect filters compacts creates update request and updates dynamodb`() = runTest {
        // arrange
        val faultManager = faultManager()
        val client = mockk<DynamoDbClient>()
        val clientFactory = mockk<DynamoDbClientFactory>()
        every { clientFactory.getClient(any()) } returns client
        val options = DynamoDbConnector.Options(dynamoDbClientFactory = clientFactory)

        val skippedByCompact = UnitOfWork(event = event("PackageCreated"), key = "skip")
        val materialized = UnitOfWork(event = event("PackageCreated"), key = "keep")
        val updateRequest = updateRequest("package-1")
        val updateResponse = UpdateItemResponse {}
        val updateRequestCalls = mutableListOf<UnitOfWork>()

        coEvery { client.updateItem(updateRequest) } returns updateResponse

        val pipeline = MaterializePipeline(
            pipelineId = "materialize-packages",
            compact = { flow: Flow<UnitOfWork> -> flow.filter {
                    it.key == "keep"
                } },
            toUpdateRequest = {
                updateRequestCalls += it
                updateRequest
            },
            dynamoDbConnectorOptions = options,
        )

        // act
        val result = pipeline
            .connect(faultManager, listOf(skippedByCompact, materialized).asFlow())
            .toList()

        // assert
        result.map { it.key } shouldContainExactly listOf("keep")
        result.single().updateRequest shouldBe updateRequest
        result.single().updateResponse!! shouldBe updateResponse
        updateRequestCalls shouldContainExactly listOf(materialized)

        coVerify(exactly = 1) {
            client.updateItem(updateRequest)
        }
    }

    @Test
    fun `connect drops units of work before update creation when event or content filters do not match`() = runTest {
        // arrange
        val faultManager = faultManager()
        val client = mockk<DynamoDbClient>()
        val clientFactory = mockk<DynamoDbClientFactory>()
        every { clientFactory.getClient(any()) } returns client
        val options = DynamoDbConnector.Options(dynamoDbClientFactory = clientFactory)

        val matching = UnitOfWork(event = event("PackageCreated"), key = "keep")
        val wrongEvent = UnitOfWork(event = event("PackageCancelled"), key = "keep")
        val wrongContent = UnitOfWork(event = event("PackageCreated"), key = "drop")
        val updateRequest = updateRequest("package-1")
        val updateRequestCalls = mutableListOf<UnitOfWork>()

        coEvery { client.updateItem(updateRequest) } returns UpdateItemResponse {}

        val pipeline = MaterializePipeline(
            pipelineId = "materialize-packages",
            eventFilter = EventFilter.ByName("PackageCreated"),
            onContentType = { it.key == "keep" },
            toUpdateRequest = {
                updateRequestCalls += it
                updateRequest
            },
            dynamoDbConnectorOptions = options,
        )

        // act
        val result = pipeline
            .connect(faultManager, listOf(matching, wrongEvent, wrongContent).asFlow())
            .toList()

        // assert
        result.map { it.key } shouldContainExactly listOf("keep")
        updateRequestCalls shouldContainExactly listOf(matching)

        coVerify(exactly = 1) { client.updateItem(updateRequest) }
    }

    @Test
    fun `connect records fault and continues when update request creation fails`() = runTest {
        // arrange
        val faultManager = faultManager()
        val client = mockk<DynamoDbClient>()
        val clientFactory = mockk<DynamoDbClientFactory>()
        every { clientFactory.getClient(any()) } returns client
        val options = DynamoDbConnector.Options(dynamoDbClientFactory = clientFactory)

        val failing = UnitOfWork(event = event("PackageCreated"), key = "fail")
        val passing = UnitOfWork(event = event("PackageCreated"), key = "pass")
        val updateRequest = updateRequest("package-1")

        coEvery { client.updateItem(updateRequest) } returns UpdateItemResponse {}

        val pipeline = MaterializePipeline(
            pipelineId = "materialize-packages",
            toUpdateRequest = {
                if (it.key == "fail") error("cannot materialize")
                updateRequest
            },
            dynamoDbConnectorOptions = options,
        )

        // act
        val result = pipeline
            .connect(faultManager, listOf(failing, passing).asFlow())
            .toList()

        // assert
        result.map { it.key } shouldContainExactly listOf("pass")
        faultManager.getFaults() shouldHaveSize 1
        faultManager.getFaults().single().runtimeUow shouldBe failing

        coVerify(exactly = 1) { client.updateItem(updateRequest) }
    }

    private fun faultManager(): FaultManager {
        val spy = spyk(EnvironmentConfig())
        every { spy.serializationStrategy() } returns "jackson"
        GlobalRegistry.setEnvConfig(spy)
        return FaultManager(
            eventPublisher = mockk<EventPublisher>(relaxed = true),
            skipErrorLogging = true,
        )
    }

    private fun event(type: String): Event {
        val event = spyk<MyEventA>()
        every { event.eventType() } returns type
        return event
    }

    private fun updateRequest(id: String): UpdateItemRequest =
        UpdateItemRequest {
            tableName = "packages"
            key = mapOf("id" to AttributeValue.S(id))
            updateExpression = "SET materialized = :materialized"
            expressionAttributeValues = mapOf(":materialized" to AttributeValue.Bool(true))
        }
}