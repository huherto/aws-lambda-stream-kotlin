package io.kopipes.core.metrics

import io.kopipes.core.UnitOfWork
import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNotBe
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test

class MetricsExtensionsTest {

    @Test
    fun `withStepMetrics should add start and end checkpoints when enabled`(): Unit = runBlocking {
        val uow = UnitOfWork()
        val result = uow.withStepMetrics("test-step", enabled = true) {
            val waitTime = it.metrics?.timer?.checkpoints?.get("default|test-step|stream.pipeline.io.wait.time")
            waitTime shouldNotBe null
            it
        }

        val ioTime = result.metrics?.timer?.checkpoints?.get("default|test-step|stream.pipeline.io.time")
        ioTime shouldNotBe null
    }

    @Test
    fun `withStepMetrics should not add checkpoints when disabled`(): Unit = runBlocking {
        val uow = UnitOfWork()
        val result = uow.withStepMetrics("test-step", enabled = false) {
            val waitTime = it.metrics?.timer?.checkpoints?.get("default|test-step|stream.pipeline.io.wait.time")
            waitTime shouldBe null
            it
        }

        val ioTime = result.metrics?.timer?.checkpoints?.get("default|test-step|stream.pipeline.io.time")
        ioTime shouldBe null
    }
}
