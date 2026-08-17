package io.github.huherto.awsLambdaStream.metrics

import io.github.huherto.awsLambdaStream.EnvironmentConfig
import io.github.huherto.awsLambdaStream.GlobalRegistry
import io.github.huherto.awsLambdaStream.UnitOfWork
import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNotBe
import io.mockk.every
import io.mockk.mockk
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test

class MetricsExtensionsTest {

    @Test
    fun `withStepMetrics should add start and end checkpoints when enabled`(): Unit = runBlocking {
        val envConfig = mockk<EnvironmentConfig>()
        every { envConfig.isMetricEnabled("step") } returns true
        GlobalRegistry.setEnvConfig(envConfig)

        val uow = UnitOfWork()
        val result = uow.withStepMetrics("test-step") {
            val waitTime = it.metrics?.timer?.checkpoints?.get("default|test-step|stream.pipeline.io.wait.time")
            waitTime shouldNotBe null
            it
        }

        val ioTime = result.metrics?.timer?.checkpoints?.get("default|test-step|stream.pipeline.io.time")
        ioTime shouldNotBe null
    }

    @Test
    fun `withStepMetrics should not add checkpoints when disabled`(): Unit = runBlocking {
        val envConfig = mockk<EnvironmentConfig>()
        every { envConfig.isMetricEnabled("step") } returns false
        GlobalRegistry.setEnvConfig(envConfig)

        val uow = UnitOfWork()
        val result = uow.withStepMetrics("test-step") {
            val waitTime = it.metrics?.timer?.checkpoints?.get("default|test-step|stream.pipeline.io.wait.time")
            waitTime shouldBe null
            it
        }

        val ioTime = result.metrics?.timer?.checkpoints?.get("default|test-step|stream.pipeline.io.time")
        ioTime shouldBe null
    }
}
