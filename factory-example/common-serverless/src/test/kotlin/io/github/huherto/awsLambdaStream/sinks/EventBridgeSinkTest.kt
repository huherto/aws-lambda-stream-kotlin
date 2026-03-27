package io.github.huherto.awsLambdaStream.sinks

import aws.sdk.kotlin.services.eventbridge.model.PutEventsRequest
import aws.sdk.kotlin.services.eventbridge.model.PutEventsRequestEntry
import io.github.huherto.awsLambdaStream.EnvironmentConfig
import io.github.huherto.awsLambdaStream.Event
import io.github.huherto.awsLambdaStream.UnitOfWork
import io.github.huherto.awsLambdaStream.connectors.ConnectorResponse
import io.github.huherto.awsLambdaStream.connectors.EventBridgeConnector
import io.github.huherto.awsLambdaStream.flavors.Pipeline
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.nulls.shouldBeNull
import io.kotest.matchers.nulls.shouldNotBeNull
import io.kotest.matchers.shouldBe
import io.mockk.*

class EventBridgeSinkTest : FunSpec({

    test("EventBridgePublishOptions should correctly use values from envConfig") {
        // Arrange
        val envConfig = mockk<EnvironmentConfig>()
        every { envConfig.busName() } returns "env-bus"
        every { envConfig.busSource() } returns "env-source"
        every { envConfig.maxPublishRequestSize() } returns 1024
        every { envConfig.publishBatchSize() } returns 5
        every { envConfig.publishParallel() } returns 4
        every { envConfig.busEndPointId() } returns "env-endpoint"

        // Act
        val options = EventBridgePublishOptions(envConfig)

        // Assert
        options.busName shouldBe "env-bus"
        options.source shouldBe "env-source"
        options.maxPublishRequestSize shouldBe 1024
        options.batchSize shouldBe 5
        options.parallel shouldBe 4
        options.endpointId shouldBe "env-endpoint"
    }

    test("EventBridgePublishOptions should use fallback defaults when envConfig returns null") {
        // Arrange
        val envConfig = mockk<EnvironmentConfig>()
        every { envConfig.busName() } returns null
        every { envConfig.busSource() } returns null
        every { envConfig.maxPublishRequestSize() } returns null
        every { envConfig.maxRequestSize() } returns null
        every { envConfig.publishBatchSize() } returns null
        every { envConfig.batchSize() } returns null
        every { envConfig.publishParallel() } returns null
        every { envConfig.parallel() } returns null
        every { envConfig.busEndPointId() } returns null

        // Act
        val options = EventBridgePublishOptions(envConfig = envConfig)

        // Assert
        options.busName shouldBe "undefined"
        options.source shouldBe "custom"
        options.maxPublishRequestSize shouldBe (256 * 1024)
        options.batchSize shouldBe 10
        options.parallel shouldBe 8
        options.endpointId.shouldBeNull()
    }

    test("toPublishRequestEntry should generate PutEventsRequestEntry from event or return unmodified UOW if event is null") {
        // Arrange
        val mockEvent = mockk<Event> {
            every { eventType() } returns "TestEvent"
            every { encoded() } returns """{"data": "test"}"""
        }
        
        val uowWithEvent = UnitOfWork(event = mockEvent)
        val uowWithoutEvent = UnitOfWork(event = null)
        
        val envConfig = mockk<EnvironmentConfig>(relaxed = true)
        val options = EventBridgePublishOptions(envConfig, busName = "custom-bus", source = "custom-source")

        // Act
        val resultWithEvent = EventBridgeSink.toPublishRequestEntry(uowWithEvent, options)
        val resultWithoutEvent = EventBridgeSink.toPublishRequestEntry(uowWithoutEvent, options)

        // Assert
        resultWithEvent.publishRequestEntry.shouldNotBeNull()
        resultWithEvent.publishRequestEntry.eventBusName shouldBe "custom-bus"
        resultWithEvent.publishRequestEntry.source shouldBe "custom-source"
        resultWithEvent.publishRequestEntry.detailType shouldBe "TestEvent"
        resultWithEvent.publishRequestEntry.detail shouldBe """{"data": "test"}"""
        
        resultWithoutEvent.publishRequestEntry.shouldBeNull()
        resultWithoutEvent shouldBe uowWithoutEvent
    }

    test("toPublishRequest should combine batch entries into a single PutEventsRequest or ignore empty entries") {
        // Arrange
        val entry1 = PutEventsRequestEntry { detail = "event1" }
        val entry2 = PutEventsRequestEntry { detail = "event2" }
        
        val batch = listOf(
            UnitOfWork(publishRequestEntry = entry1),
            UnitOfWork(publishRequestEntry = entry2),
            UnitOfWork(publishRequestEntry = null) // Should be ignored
        )
        val batchUow = UnitOfWork(batch = batch)
        val emptyBatchUow = UnitOfWork(batch = null)
        
        val envConfig = mockk<EnvironmentConfig>(relaxed = true)
        val options = EventBridgePublishOptions(envConfig, endpointId = "test-endpoint")

        // Act
        val result = EventBridgeSink.toPublishRequest(batchUow, options)
        val emptyResult = EventBridgeSink.toPublishRequest(emptyBatchUow, options)

        // Assert
        result.publishRequest.shouldNotBeNull()
        result.publishRequest.entries?.size shouldBe 2
        result.publishRequest.entries?.get(0)?.detail shouldBe "event1"
        result.publishRequest.entries?.get(1)?.detail shouldBe "event2"
        result.publishRequest.endpointId shouldBe "test-endpoint"

        emptyResult.publishRequest.shouldNotBeNull()
        emptyResult.publishRequest.entries.shouldBeNull()
    }

    test("putEvents should invoke EventBridgeConnector when publishRequest is present, and return UOW as is otherwise") {
        // Arrange
        mockkConstructor(EventBridgeConnector::class)
        val publishReq = PutEventsRequest { }
        val pipelineMock = mockk<Pipeline> {
            every { id } returns "pipeline-1"
        }
        
        val batchUowWithReq = UnitOfWork(publishRequest = publishReq, pipeline = pipelineMock)
        val batchUowWithoutReq = UnitOfWork(publishRequest = null)
        
        val envConfig = mockk<EnvironmentConfig>(relaxed = true)
        val options = EventBridgePublishOptions(envConfig)
        
        val mockResponse = mockk<ConnectorResponse>()
        coEvery { anyConstructed<EventBridgeConnector>().putEvents(any()) } returns mockResponse

        // Act
        val resultWithReq = EventBridgeSink.putEvents(batchUowWithReq, options)
        val resultWithoutReq = EventBridgeSink.putEvents(batchUowWithoutReq, options)

        // Assert
        resultWithReq.publishResponse shouldBe mockResponse
        coVerify(exactly = 1) { anyConstructed<EventBridgeConnector>().putEvents(publishReq) }
        
        resultWithoutReq.publishResponse.shouldBeNull()
        
        // Cleanup
        unmockkConstructor(EventBridgeConnector::class)
    }
})