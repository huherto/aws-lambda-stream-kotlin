package io.github.huherto.awsLambdaStream.from

import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNotBe
import kotlinx.coroutines.flow.toList

class CwAdapterSpec : StringSpec({
    "fromAlarm should wrap alarm event" {
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
})
