package io.github.huherto.awsLambdaStream.from

import com.amazonaws.services.lambda.runtime.events.SNSEvent
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
import org.joda.time.DateTime
import org.junit.jupiter.api.Test

class SnsAdapterTest {

    private val envConfig = spyk(EnvironmentConfig()).apply {
        every { serializationStrategy() } returns "jackson"
    }

    private fun faultManager() = FaultManager(
        envConfig = envConfig,
        eventPublisher = EventPublisherInMemory(),
        skipErrorLogging = true,
    )

    private fun adapter(
        faultManager: FaultManager = faultManager(),
        eventCodec: EventCodec = MyEventCodec(),
    ) = SnsAdapter(faultManager, eventCodec)

    private fun createSnsRecord(
        message: String,
        messageId: String = "id",
        timestamp: DateTime = DateTime.now()
    ): SNSEvent.SNSRecord {
        return SNSEvent.SNSRecord().apply {
            setSns(SNSEvent.SNS().apply {
                this.message = message
                this.messageId = messageId
                this.timestamp = timestamp
            })
        }
    }

    @Test
    fun `fromSns should wrap records without decoding`() = runBlocking {
        val adapter = adapter()
        val snsRecord = createSnsRecord(message = "foo")
        val snsEvent = SNSEvent().apply {
            records = listOf(snsRecord)
        }

        val results = adapter.fromSns(snsEvent).toList()

        results.shouldHaveSize(1)
        results[0].record shouldBe snsRecord
        results[0].event shouldBe null
    }

    @Test
    fun `fromSnsEvent should decode records and preserve message id`() = runBlocking {
        val adapter = adapter()
        val now = DateTime.now()
        val snsRecord = createSnsRecord(
            message = """{"type":"MY_EVENT_A","foo":"bar"}""",
            messageId = "sns-msg-id",
            timestamp = now
        )
        val snsEvent = SNSEvent().apply {
            records = listOf(snsRecord)
        }

        val results = adapter.fromSnsEvent(snsEvent).toList()

        results.shouldHaveSize(1)
        results[0].record shouldBe snsRecord
        results[0].event?.id shouldBe "sns-msg-id"
        results[0].event?.eventType() shouldBe "MY_EVENT_A"
        results[0].event?.timestamp shouldBe now.millis
    }

    @Test
    fun `fromSnsEvent should handle decode failures`() = runBlocking {
        val faultManager = faultManager()
        val adapter = adapter(faultManager = faultManager)
        val snsRecord = createSnsRecord(message = """{"type":"UNKNOWN"}""")
        val snsEvent = SNSEvent().apply {
            records = listOf(snsRecord)
        }

        val results = adapter.fromSnsEvent(snsEvent).toList()

        results.shouldHaveSize(0)
        faultManager.getFaults().shouldHaveSize(1)
    }
}
