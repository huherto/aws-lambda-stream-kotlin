package io.github.huherto.awsLambdaStream.utils

import io.github.huherto.awsLambdaStream.EnvironmentConfig
import io.github.huherto.awsLambdaStream.FaultEvent
import io.github.huherto.awsLambdaStream.FaultManager
import io.github.huherto.awsLambdaStream.UnitOfWork
import io.github.huherto.awsLambdaStream.flavors.Pipeline
import io.kotest.matchers.shouldBe
import io.mockk.every
import io.mockk.mockk
import kotlinx.coroutines.flow.Flow
import org.junit.jupiter.api.Test

class TagsTest {

    @Test
    fun `envTags should return configured environment tags`() {
        val envConfig = mockk<EnvironmentConfig>()

        every { envConfig.accountName() } returns "test-account"
        every { envConfig.region() } returns "eu-west-1"
        every { envConfig.stage() } returns "dev"
        every { envConfig.service() } returns "test-service"
        every { envConfig.awsLambdaFunctionName() } returns "test-function"

        envTags(envConfig, "test-pipeline") shouldBe mapOf(
            "account" to "test-account",
            "region" to "eu-west-1",
            "stage" to "dev",
            "source" to "test-service",
            "functionname" to "test-function",
            "pipeline" to "test-pipeline",
        )
    }

    @Test
    fun `envTags should use undefined for missing values`() {
        val envConfig = mockk<EnvironmentConfig>()

        every { envConfig.accountName() } returns null
        every { envConfig.region() } returns null
        every { envConfig.stage() } returns null
        every { envConfig.serverlessStage() } returns null
        every { envConfig.service() } returns null
        every { envConfig.project() } returns null
        every { envConfig.serverlessProject() } returns null
        every { envConfig.awsLambdaFunctionName() } returns null

        envTags(envConfig, null) shouldBe mapOf(
            "account" to "undefined",
            "region" to "undefined",
            "stage" to "undefined",
            "source" to "undefined",
            "functionname" to "undefined",
            "pipeline" to "undefined",
        )
    }

    @Test
    fun `adornStandardTags should add environment skip and pipeline tags to event`() {
        val envConfig = mockk<EnvironmentConfig>()
        val pipeline = object : Pipeline("test-pipeline", envConfig) {
            override fun connect(fm: FaultManager, fromFlow: Flow<UnitOfWork>): Flow<UnitOfWork> = fromFlow
        }
        val event = FaultEvent().apply {
            tags = mapOf("custom" to "value")
        }
        val uow = UnitOfWork(
            pipeline = pipeline,
            event = event,
        )

        every { envConfig.accountName() } returns "test-account"
        every { envConfig.region() } returns "eu-west-1"
        every { envConfig.stage() } returns "dev"
        every { envConfig.service() } returns "test-service"
        every { envConfig.awsLambdaFunctionName() } returns "test-function"
        every { envConfig.skip() } returns true

        val result = adornStandardTags(envConfig, uow)

        result shouldBe uow
        event.tags shouldBe mapOf(
            "account" to "test-account",
            "region" to "eu-west-1",
            "stage" to "dev",
            "source" to "test-service",
            "functionname" to "test-function",
            "pipeline" to "test-pipeline",
            "skip" to "true",
            "custom" to "value",
        )
    }

    @Test
    fun `adornStandardTags should return unchanged unit of work when event is missing`() {
        val envConfig = mockk<EnvironmentConfig>()
        val uow = UnitOfWork()

        adornStandardTags(envConfig, uow) shouldBe uow
    }

}