package io.github.huherto.awsLambdaStream.from

import com.amazonaws.services.lambda.runtime.events.ScheduledEvent
import io.github.huherto.awsLambdaStream.EnvironmentConfig
import io.github.huherto.awsLambdaStream.EventCodec
import io.github.huherto.awsLambdaStream.FaultManager
import io.github.huherto.awsLambdaStream.MyEventCodec
import io.github.huherto.awsLambdaStream.sinks.EventPublisherInMemory
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.shouldBe
import io.mockk.every
import io.mockk.spyk
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test

class EventBridgeAdapterTest {

    private val envConfig = spyk(EnvironmentConfig()).apply {
        every { serializationStrategy() } returns "jackson"
    }

    private fun faultManager() = FaultManager(
        eventPublisher = EventPublisherInMemory(),
        skipErrorLogging = true,
    )

    private fun adapter(
        faultManager: FaultManager = faultManager(),
        eventCodec: EventCodec = MyEventCodec(),
    ) = EventBridgeAdapter(faultManager, eventCodec)

    @Test
    fun `fromEventBridge should decode detail and preserve event id`() = runBlocking {
        val adapter = adapter()
        val eventBridgeEvent = ScheduledEvent().apply {
            id = "event-id"
            source = "aws.events"
            detail = mapOf(
                "type" to "MY_EVENT_A",
                "foo" to "bar",
                "id" to "inner-id"
            )
        }

        val results = adapter.fromEventBridge(eventBridgeEvent).toList()

        results.shouldHaveSize(1)
        results[0].record shouldBe eventBridgeEvent
        results[0].event?.id shouldBe "inner-id"
        results[0].event?.eventType() shouldBe "MY_EVENT_A"
    }

    @Test
    fun `fromEventBridge should use record id if event id is missing`() = runBlocking {
        val adapter = adapter()
        val eventBridgeEvent = ScheduledEvent().apply {
            id = "event-id"
            source = "aws.events"
            detail = mapOf(
                "type" to "MY_EVENT_A",
                "foo" to "bar"
            )
        }

        val results = adapter.fromEventBridge(eventBridgeEvent).toList()

        results.shouldHaveSize(1)
        results[0].record shouldBe eventBridgeEvent
        results[0].event?.id shouldBe "event-id"
    }

    @Test
    fun `fromEventBridge should handle decode failures`() = runBlocking {
        val faultManager = faultManager()
        val adapter = adapter(faultManager = faultManager)
        val eventBridgeEvent = ScheduledEvent().apply {
            id = "event-id"
            source = "aws.events"
            detail = mapOf(
                "type" to "UNKNOWN_EVENT"
            )
        }

        val results = adapter.fromEventBridge(eventBridgeEvent).toList()

        results.shouldHaveSize(0)
        faultManager.getFaults().shouldHaveSize(1)
        faultManager.getFaults()[0].runtimeUow?.record shouldBe eventBridgeEvent
    }

    @Test
    fun `fromScheduledEvent should wrap event`() = runBlocking {
        val adapter = adapter()
        val scheduledEvent = ScheduledEvent().apply {
            id = "scheduled-id"
            source = "aws.events"
        }

        val results = adapter.fromScheduledEvent(scheduledEvent).toList()

        results.shouldHaveSize(1)
        results[0].record shouldBe scheduledEvent
        results[0].event?.id shouldBe "scheduled-id"
    }
}
