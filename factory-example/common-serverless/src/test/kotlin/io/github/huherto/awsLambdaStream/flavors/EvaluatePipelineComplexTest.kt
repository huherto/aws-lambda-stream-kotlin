package io.github.huherto.awsLambdaStream.flavors

import aws.sdk.kotlin.services.dynamodb.DynamoDbClient
import aws.sdk.kotlin.services.dynamodb.model.QueryResponse
import io.github.huherto.awsLambdaStream.EnvironmentConfig
import io.github.huherto.awsLambdaStream.Event
import io.github.huherto.awsLambdaStream.UnitOfWork
import io.github.huherto.awsLambdaStream.sinks.EventPublisherInMemory
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.collections.shouldBeEmpty
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.nulls.shouldNotBeNull
import io.kotest.matchers.shouldBe
import io.mockk.coEvery
import io.mockk.every
import io.mockk.mockk
import io.mockk.spyk
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.toList
import aws.sdk.kotlin.services.dynamodb.model.AttributeValue as SdkAV

class EvaluatePipelineComplexTest : FunSpec({

    context("when expression is null") {

        test("should assign triggers based on the presence of the event") {
            // Arrange
            val envConfig = spyk<EnvironmentConfig>()
            val eventPublisher = EventPublisherInMemory()
            val pipeline = EvaluatePipeline(
                id="pipeline-1",
                envConfig=envConfig,
                eventPublisher = eventPublisher,
                expression=null)
            val mockEvent = mockk<Event>()
            val uowWithEvent = UnitOfWork(event = mockEvent)
            val uowWithoutEvent = UnitOfWork(event = null)

            // Act
            // Since `complex()` is a member extension function, we call it inside `with(pipeline)`
            val resultsWithEvent = with(pipeline) { flowOf(uowWithEvent).complex().toList() }
            val resultsWithoutEvent = with(pipeline) { flowOf(uowWithoutEvent).complex().toList() }

            // Assert
            resultsWithEvent shouldHaveSize 1
            resultsWithEvent.first().triggers shouldBe listOf(mockEvent)

            resultsWithoutEvent shouldHaveSize 1
            resultsWithoutEvent.first().triggers.shouldBeEmpty()
        }
    }

    context("when expression is provided (complex correlation pipeline)") {

        test("should execute correlation pipeline successfully and filter by suffix") {
            // Arrange
            val mockDynamoDbClient = spyk<DynamoDbClient>()
            val envConfig = spyk<EnvironmentConfig>()
            val eventPublisher = EventPublisherInMemory()

            // Mocking DynamoDB client to return two simulated events
            coEvery { mockDynamoDbClient.query(any()) } returns QueryResponse {
                items = listOf(
                    mapOf("event" to SdkAV.S("""{"id": "e1"}""")),
                    mapOf("event" to SdkAV.S("""{"id": "e2"}"""))
                )
            }

            val pipeline = EvaluatePipeline(
                id = "pipeline-2",
                envConfig=envConfig,
                eventPublisher = eventPublisher,
                dynamoDbClient = mockDynamoDbClient,
                correlationKeySuffix = "expected-suffix",
                unmarshall = { str -> mockk<Event> { every { encoded() } returns str } },
                expression = { uow -> uow.correlated?.size == 2 } // Keep UOW only if exactly 2 correlated events found
            )

            // UnitOfWork missing the correct suffix (should drop early)
            val uowInvalidSuffix = UnitOfWork(meta = mapOf("suffix" to "wrong-suffix"))

            // Valid UnitOfWork that should pass the entire pipeline
            val uowValid = UnitOfWork(
                meta = mapOf(
                    "suffix" to "expected-suffix",
                    "correlation" to "true",
                    "pk" to "test-pk"
                )
            )

            // Act
            val resultsInvalidSuffix = with(pipeline) { flowOf(uowInvalidSuffix).complex().toList() }
            val resultsValid = with(pipeline) { flowOf(uowValid).complex().toList() }

            // Assert
            // 1. Should be filtered out by `onCorrelationKeySuffix`
            resultsInvalidSuffix.shouldBeEmpty()

            // 2. Should be processed through query request, dynamoDb fetching, and expression filter
            resultsValid shouldHaveSize 1
            val processedUow = resultsValid.first()

            processedUow.queryRequest.shouldNotBeNull()
            processedUow.queryRequest.keyConditionExpression shouldBe "#pk = :pk"

            processedUow.correlated.shouldNotBeNull()
            processedUow.correlated shouldHaveSize 2
            processedUow.correlated[0].encoded() shouldBe """{"id": "e1"}"""
            processedUow.correlated[1].encoded() shouldBe """{"id": "e2"}"""
        }

        test("should filter out UOWs if the expression block evaluates to false") {
            // Arrange
            val mockDynamoDbClient = spyk<DynamoDbClient>()
            val envConfig = spyk<EnvironmentConfig>()
            val eventPublisher = EventPublisherInMemory()

            // Mocking an empty DynamoDB response (no correlations found)
            coEvery { mockDynamoDbClient.query(any()) } returns QueryResponse { items = emptyList() }

            val pipeline = EvaluatePipeline(
                id = "pipeline-3",
                envConfig=envConfig,
                eventPublisher = eventPublisher,
                dynamoDbClient = mockDynamoDbClient,
                correlationKeySuffix = "expected-suffix",
                unmarshall = { mockk<Event>() },
                expression = { uow -> uow.correlated?.isNotEmpty() == true } // Fails for an empty correlated list
            )

            val uow = UnitOfWork(meta = mapOf("suffix" to "expected-suffix"))

            // Act
            val results = with(pipeline) { flowOf(uow).complex().toList() }

            // Assert
            results.shouldBeEmpty()
        }

        test("should throw an IllegalStateException if dynamoDbClient is not configured") {
            // Arrange
            val envConfig = spyk<EnvironmentConfig>()
            val eventPublisher = EventPublisherInMemory()
            val pipeline = EvaluatePipeline(
                id = "pipeline-4",
                envConfig=envConfig,
                eventPublisher = eventPublisher,
                dynamoDbClient = null, // Missing client
                expression = { true }
            )

            val uow = UnitOfWork(meta = mapOf("suffix" to "")) // Empty suffix matches empty pipeline suffix

            // Act & Assert
            val exception = shouldThrow<IllegalStateException> {
                with(pipeline) { flowOf(uow).complex().toList() }
            }

            exception.message shouldBe "DynamoDB client must be configured to process expressions"
        }
    }
})