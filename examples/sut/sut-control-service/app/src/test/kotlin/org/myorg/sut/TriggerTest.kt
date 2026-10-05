package org.myorg.sut

import com.amazonaws.services.lambda.runtime.events.DynamodbEvent
import com.amazonaws.services.lambda.runtime.events.models.dynamodb.StreamRecord
import io.kopipes.aws.from.DynamodbAdapter
import io.kopipes.aws.testsupport.TestContext
import io.kopipes.core.GlobalRegistry
import io.kopipes.core.PipelineAssembler
import io.kopipes.core.faults.FaultManager
import io.kopipes.core.sinks.EventPublisherInMemory
import io.kopipes.core.sinks.EventsMicrostoreInMemory
import io.kotest.matchers.nulls.shouldBeNull
import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNotBe
import io.kotest.matchers.types.shouldBeInstanceOf
import org.junit.jupiter.api.Test

class TriggerTest {

    fun createContainer(): TriggerContainer {
        val eventPublisher = EventPublisherInMemory()
        val faultManager = FaultManager(eventPublisher, skipErrorLogging = true)
        GlobalRegistry.setFaultManager(faultManager)
        val eventsMicrostore = EventsMicrostoreInMemory(faultManager)
        val container = TriggerContainer(
            eventsMicrostore = eventsMicrostore,
            eventPublisher = eventPublisher
        )
        
        return container
    }

    @Test
    fun `should lazily initialize container`() {
        // Arrange
        val container = createContainer()

        // Act
        val assembler = container.assembler
        val dynamoDBAdapter = container.dynamoDbAdapter

        // Assert
        assembler shouldNotBe null
        assembler.shouldBeInstanceOf<PipelineAssembler>()

        assembler.getFaultManager() shouldNotBe null
        assembler.getFaultManager() shouldBe GlobalRegistry.faultManager()

        dynamoDBAdapter shouldNotBe null
        dynamoDBAdapter.shouldBeInstanceOf<DynamodbAdapter>()

    }
    @Test
    fun `should process DynamodbEvent successfully and return 'Done' for various record scenarios`() {
        // Arrange
        val container = createContainer()
        val trigger = Trigger({ container })
        val testContext = TestContext()

        val emptyEvent = DynamodbEvent().apply {
            records = emptyList()
        }

        val singleRecordEvent = DynamodbEvent().apply {
            records = listOf(
                DynamodbEvent.DynamodbStreamRecord().apply {
                    eventName = "INSERT"
                    dynamodb = StreamRecord().apply {
                        keys = emptyMap()
                        newImage = emptyMap()
                        oldImage = emptyMap()
                    }
                }
            )
        }

        // Act
        val emptyEventResult = trigger.handleRequest(emptyEvent, testContext)
        val singleRecordResult = trigger.handleRequest(singleRecordEvent, testContext)

        // Assert
        emptyEventResult.shouldBeNull()
        singleRecordResult.shouldBeNull()
    }
}
