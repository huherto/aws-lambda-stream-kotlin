package io.github.huherto.awsLambdaStream.from

import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNotBe
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test

class CwAdapterTest {
    @Test
    fun `fromAlarm should wrap alarm event`() = runBlocking {
        val adapter = CwAdapter()
        val alarmEvent = mapOf("AlarmName" to "MyAlarm")

        val results = adapter.fromAlarm(alarmEvent).toList()

        results.size shouldBe 1
        results[0].record shouldBe alarmEvent
        val event = results[0].event as AlarmEvent
        event.id shouldNotBe null
        event.eventType() shouldBe "aws-cloudwatch-alarm"
        event.record shouldBe alarmEvent
    }
}
