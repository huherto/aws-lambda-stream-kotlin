package org.myorg.sut

import com.amazonaws.services.lambda.runtime.events.DynamodbEvent
import com.amazonaws.services.lambda.runtime.events.models.dynamodb.StreamRecord
import io.kopipes.aws.GlobalRegistry
import io.kopipes.aws.PipelineAssembler
import io.kopipes.aws.faults.FaultManager
import io.kopipes.aws.from.DynamodbAdapter
import io.kopipes.aws.sinks.EventPublisherInMemory
import io.kopipes.aws.sinks.EventsMicrostoreInMemory
import io.kopipes.aws.testsupport.TestContext
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
        val container = TriggerContainer(eventPublisher, eventsMicrostore)
        
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
        emptyEventResult shouldBe "Done"
        singleRecordResult shouldBe "Done"
    }
}
