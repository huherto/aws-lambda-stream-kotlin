package io.github.huherto.awsLambdaStream.connectors

import aws.sdk.kotlin.services.cloudwatch.CloudWatchClient
import aws.sdk.kotlin.services.cloudwatch.model.PutMetricDataRequest
import aws.sdk.kotlin.services.cloudwatch.model.PutMetricDataResponse
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.shouldBe
import io.mockk.*
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import java.io.IOException

class CloudWatchConnectorTest {

    @AfterEach
    fun tearDown() {
        clearAllMocks()
    }

    @Test
    fun `putMetricData returns client response`() = runTest {
        // Arrange
        val client = mockk<CloudWatchClient>()
        val clientFactory = mockk<CloudWatchClientFactory>()
        val response = PutMetricDataResponse { }
        val request = PutMetricDataRequest { namespace = "Test" }

        every { clientFactory.getClient("pipeline-1") } returns client
        coEvery { client.putMetricData(request) } returns response

        val connector = CloudWatchConnector(
            pipelineId = "pipeline-1",
            envConfig = mockk(relaxed = true),
            clientFactory = clientFactory
        )

        // Act
        val result = connector.putMetricData(request)

        // Assert
        result shouldBe response
        coVerify(exactly = 1) { client.putMetricData(request) }
    }

    @Test
    fun `putMetricData rethrows client exceptions`() = runTest {
        // Arrange
        val client = mockk<CloudWatchClient>()
        val clientFactory = mockk<CloudWatchClientFactory>()
        val exception = IOException("boom")
        val request = PutMetricDataRequest { namespace = "Test" }

        every { clientFactory.getClient("pipeline-1") } returns client
        coEvery { client.putMetricData(request) } throws exception

        val connector = CloudWatchConnector(
            pipelineId = "pipeline-1",
            envConfig = mockk(relaxed = true),
            clientFactory = clientFactory
        )

        // Act
        val thrown = shouldThrow<IOException> {
            connector.putMetricData(request)
        }

        // Assert
        thrown shouldBe exception
        coVerify(exactly = 1) { client.putMetricData(request) }
    }
}
