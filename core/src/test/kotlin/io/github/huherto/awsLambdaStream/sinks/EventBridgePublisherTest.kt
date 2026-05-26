package io.github.huherto.awsLambdaStream.sinks

import aws.sdk.kotlin.services.eventbridge.model.PutEventsRequest
import aws.sdk.kotlin.services.eventbridge.model.PutEventsRequestEntry
import io.github.huherto.awsLambdaStream.EnvironmentConfig
import io.github.huherto.awsLambdaStream.Event
import io.github.huherto.awsLambdaStream.UnitOfWork
import io.github.huherto.awsLambdaStream.connectors.ConnectorResponse
import io.github.huherto.awsLambdaStream.connectors.EventBridgeConnector
import io.kotest.matchers.nulls.shouldBeNull
import io.kotest.matchers.nulls.shouldNotBeNull
import io.kotest.matchers.shouldBe
import io.mockk.*
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test

class EventBridgePublisherTest {


    fun mockEnvConfig() : EnvironmentConfig {
        val spy = spyk(EnvironmentConfig())
        every { spy.awsRegion() } returns "us-east-1"
        return spy
    }

    @AfterEach
    fun tearDown() {
        unmockkAll()
    }

    @Nested
    inner class ToPublishRequestEntry {

        @Test
        fun `should map correctly based on event presence`() {
            // Arrange
//            val opt = EventBridgePublishOptions(
//                envConfig = mockEnvConfig(),
//                busName = "test-bus",
//                source = "test-source"
//            )
            val publisher = EventBridgePublisher(
                mockEnvConfig(),
                busName = "test-bus",
                source = "test-source")

            val mockEvent = mockk<Event>()
            every { mockEvent.eventType() } returns "test-type"
            every { mockEvent.encoded() } returns """{"data":"value"}"""

            val uowWithEvent = UnitOfWork(event = mockEvent)
            val uowWithoutEvent = UnitOfWork(event = null)

            // Act
            val resultWithEvent = publisher.toPublishRequestEntry(uowWithEvent)
            val resultWithoutEvent = publisher.toPublishRequestEntry(uowWithoutEvent)

            // Assert
            resultWithEvent.publishRequestEntry.shouldNotBeNull()
            resultWithEvent.publishRequestEntry.eventBusName shouldBe "test-bus"
            resultWithEvent.publishRequestEntry.source shouldBe "test-source"
            resultWithEvent.publishRequestEntry.detailType shouldBe "test-type"
            resultWithEvent.publishRequestEntry.detail shouldBe """{"data":"value"}"""

            resultWithoutEvent.publishRequestEntry.shouldBeNull()
        }
    }

    @Nested
    inner class ToPublishRequest {

        @Test
        fun `should handle batch correctly based on entries presence`() {
            // Arrange
//            val opt = EventBridgePublishOptions(
//                envConfig = mockEnvConfig(),
//                endpointId = "test-endpoint"
//            )
            val publisher = EventBridgePublisher(mockEnvConfig(), endpointId = "test-endpoint")

            val entry1 = PutEventsRequestEntry.Companion { source = "source1" }

            val validBatch = listOf(
                UnitOfWork(publishRequestEntry = entry1),
                UnitOfWork(publishRequestEntry = null) // this should be filtered out
            )

            val uowWithBatch = UnitOfWork(batch = validBatch)
            val uowEmptyBatch = UnitOfWork(batch = emptyList())
            val uowNullBatch = UnitOfWork(batch = null)

            // Act
            val resultWithBatch = publisher.toPublishRequest(uowWithBatch)
            val resultEmptyBatch = publisher.toPublishRequest(uowEmptyBatch)
            val resultNullBatch = publisher.toPublishRequest(uowNullBatch)

            // Assert
            resultWithBatch.publishRequest.shouldNotBeNull()
            resultWithBatch.publishRequest.entries?.size shouldBe 1
            resultWithBatch.publishRequest.entries?.get(0)?.source shouldBe "source1"
            resultWithBatch.publishRequest.endpointId shouldBe "test-endpoint"

            resultEmptyBatch.publishRequest shouldBe null
            resultNullBatch.publishRequest shouldBe null
        }
    }

    @Nested
    inner class PutEvents {

        @Test
        fun `should execute connector when publishRequest is provided`(): Unit  = runBlocking {
            // Arrange
            mockkConstructor(EventBridgeConnector::class)
            val mockResponse = mockk<ConnectorResponse>()
            coEvery { anyConstructed<EventBridgeConnector>().putEvents(any()) } returns mockResponse

            val publisher = EventBridgePublisher(mockEnvConfig())

            val request = PutEventsRequest.Companion { endpointId = "test-endpoint" }
            val uowWithRequest = UnitOfWork(publishRequest = request)
            val uowWithoutRequest = UnitOfWork(publishRequest = null)

            // Act
            val resultWithRequest = publisher.putEvents(uowWithRequest)
            val resultWithoutRequest = publisher.putEvents(uowWithoutRequest)

            // Assert
            resultWithRequest.publishResponse shouldBe mockResponse
            resultWithRequest.publishRequest shouldBe request

            resultWithoutRequest.publishResponse.shouldBeNull()
        }
    }

    @Nested
    inner class PublishFlow {

        @Test
        fun `should process flow of UnitOfWork completely`() : Unit = runBlocking {
            // Arrange
            mockkConstructor(EventBridgeConnector::class)
            val mockResponse = mockk<ConnectorResponse>()
            coEvery { anyConstructed<EventBridgeConnector>().putEvents(any()) } returns mockResponse

            val publisher = EventBridgePublisher(
                mockEnvConfig(),
                busName = "flow-bus",
                source = "flow-source",
                batchSize = 2)

            val mockEvent1 = mockk<Event>()
            every { mockEvent1.eventType() } returns "type1"
            every { mockEvent1.encoded() } returns "data1"

            val mockEvent2 = mockk<Event>()
            every { mockEvent2.eventType() } returns "type2"
            every { mockEvent2.encoded() } returns "data2"

            val uow1 = UnitOfWork(event = mockEvent1)
            val uow2 = UnitOfWork(event = mockEvent2)

            val inputFlow = flowOf(uow1, uow2)

            // Act
            val resultList = publisher.publish(inputFlow).toList()

            // Assert
            resultList.size shouldBe 2
            
            // Check that entries map correctly
            resultList[0].publishRequestEntry?.detail shouldBe "data1"
            resultList[1].publishRequestEntry?.detail shouldBe "data2"
        }
    }
}