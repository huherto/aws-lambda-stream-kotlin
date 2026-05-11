package io.github.huherto.awsLambdaStream.connectors

import aws.sdk.kotlin.services.eventbridge.EventBridgeClient
import aws.sdk.kotlin.services.eventbridge.model.PutEventsRequest
import aws.sdk.kotlin.services.eventbridge.model.PutEventsResponse
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.shouldBe
import io.mockk.*
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.io.IOException
import kotlin.time.Duration.Companion.seconds

class EventBridgeConnectorTest {

    private val retryConfig = RetryConfig(maxRetries = 2, retryWait = 1.seconds)
    private val request = PutEventsRequest { }
    private val timeout = 1.seconds

    @BeforeEach
    fun setUp() {
        MockKAnnotations.init(this)
        clearAllMocks()
    }

    @AfterEach
    fun tearDown() {
        clearAllMocks()
    }

    @Test
    fun `sendCommand captures metrics before sending and returns client response`() = runTest {
        // Arrange
        val client = mockk<EventBridgeClient>()
        val clientFactory = mockk<EventBridgeClientFactory>()
        val metrics = mockk<Metrics>()
        val response = PutEventsResponse { failedEntryCount = 0 }

        every { clientFactory.getClient("pipeline-1") } returns client
        coEvery { client.putEvents(request) } returns response
        justRun { metrics.capture(client, request, "eventbridge", any(), null) }

        val connector = EventBridgeConnector(
            pipelineId = "pipeline-1",
            envConfig = mockk(relaxed = true),
            timeout = timeout,
            retryConfig = retryConfig,
            opt = ConnectorOptions(metrics = metrics),
            clientFactory = clientFactory
        )

        // Act
        val result = connector.sendCommand(request, null)

        // Assert
        result shouldBe response
        verify(exactly = 1) { metrics.capture(client, request, "eventbridge", any(), null) }
        coVerify(exactly = 1) { client.putEvents(request) }
    }

    @Test
    fun `sendCommand rethrows client exceptions after logging and still captures metrics`() = runTest {
        // Arrange
        val client = mockk<EventBridgeClient>()
        val clientFactory = mockk<EventBridgeClientFactory>()
        val metrics = mockk<Metrics>()
        val exception = IOException("boom")

        every { clientFactory.getClient("pipeline-1") } returns client
        coEvery { client.putEvents(request) } throws exception
        justRun { metrics.capture(client, request, "eventbridge", any(), null) }

        val connector = EventBridgeConnector(
            pipelineId = "pipeline-1",
            envConfig = mockk(relaxed = true),
            timeout = timeout,
            retryConfig = retryConfig,
            opt = ConnectorOptions(metrics = metrics),
            clientFactory = clientFactory
        )

        // Act
        val thrown = shouldThrow<IOException> {
            connector.sendCommand(request, null)
        }

        // Assert
        thrown shouldBe exception
        verify(exactly = 1) { metrics.capture(client, request, "eventbridge", any(), null) }
        coVerify(exactly = 1) { client.putEvents(request) }
    }

    @Test
    fun `putEvents delegates to retry executor and returns combined connector response`() = runTest {
        // Arrange
        val client = mockk<EventBridgeClient>()
        val clientFactory = mockk<EventBridgeClientFactory>()
        val response = PutEventsResponse { failedEntryCount = 0 }

        every { clientFactory.getClient("pipeline-1") } returns client
        coEvery { client.putEvents(request) } returns response

        val connector = EventBridgeConnector(
            pipelineId = "pipeline-1",
            envConfig = mockk(relaxed = true),
            timeout = timeout,
            retryConfig = retryConfig,
            opt = ConnectorOptions(),
            clientFactory = clientFactory
        )

        // Act
        val result = connector.putEvents(request)

        // Assert
        result.failedEntryCount shouldBe 0
        result.entries shouldBe emptyList()
        result.attempts shouldBe listOf(response)
        coVerify(exactly = 1) { client.putEvents(request) }
    }
}