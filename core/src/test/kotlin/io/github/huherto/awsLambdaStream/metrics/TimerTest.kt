package io.github.huherto.awsLambdaStream.metrics

import io.kotest.matchers.shouldBe
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

class TimerTest {
    private var currentTime = 1000L

    @BeforeEach
    fun setup() {
        Timer.now = { currentTime }
    }

    @AfterEach
    fun tearDown() {
        Timer.now = { System.currentTimeMillis() }
    }

    @Test
    fun `should record checkpoints deltas`() {
        val timer = Timer(start = 1000L, last = 1000L)

        currentTime = 1100L
        val t1 = timer.checkpoint("c1")
        t1.checkpoints["c1"] shouldBe 100L
        t1.last shouldBe 1100L

        currentTime = 1300L
        val t2 = t1.checkpoint("c2")
        t2.checkpoints["c1"] shouldBe 100L
        t2.checkpoints["c2"] shouldBe 200L
        t2.last shouldBe 1300L
    }

    @Test
    fun `should record end delta from first checkpoint`() {
        val timer = Timer(start = 1000L, last = 1000L)

        currentTime = 1050L
        val t1 = timer.checkpoint("stream.channel.wait.time")
        t1.checkpoints["stream.channel.wait.time"] shouldBe 50L // 1050 - 1000

        currentTime = 1200L
        val t2 = t1.end("stream.pipeline.time")
        // end = now - (firstCheckpointValue + start) = 1200 - (50 + 1000) = 150L
        t2.checkpoints["stream.pipeline.time"] shouldBe 150L
    }
}
