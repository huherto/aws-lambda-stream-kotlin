package io.kopipes.aws.sinks

import aws.sdk.kotlin.services.cloudwatch.model.PutMetricDataRequest
import aws.sdk.kotlin.services.cloudwatch.model.PutMetricDataResponse
import io.kopipes.aws.connectors.CloudWatchConnector
import io.kopipes.aws.extensions.putMetricDataResponse
import io.kopipes.aws.extensions.withPutMetricDataRequest
import io.kopipes.core.EnvironmentConfig
import io.kopipes.core.UnitOfWork
import io.kopipes.core.faults.FaultManager
import io.kopipes.core.sinks.EventPublisherInMemory
import io.kotest.matchers.shouldBe
import io.mockk.*
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test

class CloudWatchSinkTest {

    private fun mockEnvConfig() : EnvironmentConfig {
        val spy = spyk(EnvironmentConfig())
        every { spy.awsRegion() } returns "us-east-1"
        return spy
    }

    @AfterEach
    fun tearDown() {
        unmockkAll()
    }

    @Test
    fun `should put metrics when request is present`() = runBlocking {
        // Arrange
        mockkConstructor(CloudWatchConnector::class)
        val mockResponse = PutMetricDataResponse { }
        coEvery { anyConstructed<CloudWatchConnector>().putMetricData(any()) } returns mockResponse

        val envConfig = mockEnvConfig()
        val fm = FaultManager(EventPublisherInMemory())
        val sink = CloudWatchSink()

        val request = PutMetricDataRequest {
            namespace = "TestNamespace"
        }
        val uow = UnitOfWork().withPutMetricDataRequest(request)
        val flow = flowOf(uow)

        // Act
        val result = sink.putMetrics(fm, flow).toList()

        // Assert
        result.size shouldBe 1
        result[0].putMetricDataResponse shouldBe mockResponse
        coVerify { anyConstructed<CloudWatchConnector>().putMetricData(request) }
    }

    @Test
    fun `should store response in extensions`() : Unit = runBlocking {
        // Arrange
        mockkConstructor(CloudWatchConnector::class)
        val mockResponse = PutMetricDataResponse { }
        coEvery { anyConstructed<CloudWatchConnector>().putMetricData(any()) } returns mockResponse

        val envConfig = mockEnvConfig()
        val fm = FaultManager(EventPublisherInMemory())
        val sink = CloudWatchSink()

        val request = PutMetricDataRequest {
            namespace = "TestNamespace"
        }
        val uow = UnitOfWork().withPutMetricDataRequest(request)
        val flow = flowOf(uow)

        // Act
        val result = sink.putMetrics(fm, flow).toList()

        // Assert
        result[0].putMetricDataResponse shouldBe mockResponse
        result[0].getExtension<io.kopipes.aws.extensions.CloudWatchExtensions>()?.putMetricDataResponse shouldBe mockResponse
    }

    @Test
    fun `should skip when request is missing`() : Unit = runBlocking {
        // Arrange
        val envConfig = mockEnvConfig()
        val fm = FaultManager(EventPublisherInMemory())
        val sink = CloudWatchSink()

        val uow = UnitOfWork()
        val flow = flowOf(uow)

        // Act
        val result = sink.putMetrics(fm, flow).toList()

        // Assert
        result.size shouldBe 1
        result[0].putMetricDataResponse shouldBe null
    }
}
