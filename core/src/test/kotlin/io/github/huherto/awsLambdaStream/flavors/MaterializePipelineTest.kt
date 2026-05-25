package io.github.huherto.awsLambdaStream.flavors

import aws.sdk.kotlin.services.dynamodb.model.AttributeValue
import aws.sdk.kotlin.services.dynamodb.model.UpdateItemRequest
import aws.sdk.kotlin.services.dynamodb.model.UpdateItemResponse
import io.github.huherto.awsLambdaStream.*
import io.github.huherto.awsLambdaStream.connectors.DynamoDbConnector
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
        val envConfig = EnvironmentConfig()
        val faultManager = faultManager(envConfig)
        val connector = mockk<DynamoDbConnector>()
        val skippedByCompact = UnitOfWork(event = event("PackageCreated"), key = "skip")
        val materialized = UnitOfWork(event = event("PackageCreated"), key = "keep")
        val updateRequest = updateRequest("package-1")
        val updateResponse = UpdateItemResponse {}
        val updateRequestCalls = mutableListOf<UnitOfWork>()

        coEvery { connector.update(updateRequest, any()) } returns updateResponse

        val pipeline = MaterializePipeline(
            pipelineId = "materialize-packages",
            envConfig = envConfig,
            compact = { flow: Flow<UnitOfWork> -> flow.filter {
                    it.key == "keep"
                } },
            toUpdateRequest = {
                updateRequestCalls += it
                updateRequest
            },
            dynamoDbConnector = connector,
        )

        // act
        val result = pipeline
            .connect(faultManager, listOf(skippedByCompact, materialized).asFlow())
            .toList()

        // assert
        result.map { it.key } shouldContainExactly listOf("keep")
        result.single().updateRequest shouldBe updateRequest
        result.single().updateResponse shouldBe updateResponse
        updateRequestCalls shouldContainExactly listOf(materialized)

        coVerify(exactly = 1) {
            connector.update(updateRequest, match { it.key == "keep" && it.updateRequest == updateRequest })
        }
    }

    @Test
    fun `connect drops units of work before update creation when event or content filters do not match`() = runTest {
        // arrange
        val envConfig = EnvironmentConfig()
        val faultManager = faultManager(envConfig)
        val connector = mockk<DynamoDbConnector>()
        val matching = UnitOfWork(event = event("PackageCreated"), key = "keep")
        val wrongEvent = UnitOfWork(event = event("PackageCancelled"), key = "keep")
        val wrongContent = UnitOfWork(event = event("PackageCreated"), key = "drop")
        val updateRequest = updateRequest("package-1")
        val updateRequestCalls = mutableListOf<UnitOfWork>()

        coEvery { connector.update(updateRequest, any()) } returns UpdateItemResponse {}

        val pipeline = MaterializePipeline(
            pipelineId = "materialize-packages",
            envConfig = envConfig,
            eventFilter = EventFilter.ByName("PackageCreated"),
            onContentType = { it.key == "keep" },
            toUpdateRequest = {
                updateRequestCalls += it
                updateRequest
            },
            dynamoDbConnector = connector,
        )

        // act
        val result = pipeline
            .connect(faultManager, listOf(matching, wrongEvent, wrongContent).asFlow())
            .toList()

        // assert
        result.map { it.key } shouldContainExactly listOf("keep")
        updateRequestCalls shouldContainExactly listOf(matching)

        coVerify(exactly = 1) { connector.update(updateRequest, any()) }
    }

    @Test
    fun `connect records fault and continues when update request creation fails`() = runTest {
        // arrange
        val envConfig = EnvironmentConfig()
        val faultManager = faultManager(envConfig)
        val connector = mockk<DynamoDbConnector>()
        val failing = UnitOfWork(event = event("PackageCreated"), key = "fail")
        val passing = UnitOfWork(event = event("PackageCreated"), key = "pass")
        val updateRequest = updateRequest("package-1")

        coEvery { connector.update(updateRequest, any()) } returns UpdateItemResponse {}

        val pipeline = MaterializePipeline(
            pipelineId = "materialize-packages",
            envConfig = envConfig,
            toUpdateRequest = {
                if (it.key == "fail") error("cannot materialize")
                updateRequest
            },
            dynamoDbConnector = connector,
        )

        // act
        val result = pipeline
            .connect(faultManager, listOf(failing, passing).asFlow())
            .toList()

        // assert
        result.map { it.key } shouldContainExactly listOf("pass")
        faultManager.getFaults() shouldHaveSize 1
        faultManager.getFaults().single().failureException?.uow shouldBe failing

        coVerify(exactly = 1) { connector.update(updateRequest, any()) }
    }

    private fun faultManager(envConfig: EnvironmentConfig): FaultManager =
        FaultManager(
            envConfig = envConfig,
            eventPublisher = mockk<EventPublisher>(relaxed = true),
            skipErrorLogging = true,
        )

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