package org.myorg.sut

import com.amazonaws.services.lambda.runtime.Context
import io.kotest.matchers.shouldBe
import io.mockk.*
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test

class RestHandlerTest {

    private val context = mockk<Context>(relaxed = true)

    @AfterEach
    fun tearDown() {
        clearAllMocks()
    }

    @Test
    fun `handleRequest returns health check response for routeKey GET check`() {
        // Arrange
        mockkConstructor(Model::class)

        val expectedResponse = HealthCheckResponse(
            statusCode = 200,
            timestamp = 1_717_000_000_000L,
            region = "us-east-1",
        )

        coEvery { anyConstructed<Model>().check() } returns expectedResponse

        val event = mapOf<String, Any?>(
            "routeKey" to "GET /check",
        )

        val handler = RestHandler("tracer", false, "us-east-1")

        // Act
        val response = handler.handleRequest(event, context)

        // Assert
        response shouldBe expectedResponse

        coVerify(exactly = 1) {
            anyConstructed<Model>().check()
        }
    }

    @Test
    fun `handleRequest returns health check response for legacy path-only check route`() {
        // Arrange
        mockkConstructor(Model::class)

        val expectedResponse = HealthCheckResponse(
            statusCode = 200,
            timestamp = 1_717_000_000_000L,
            region = "us-east-1",
        )

        coEvery { anyConstructed<Model>().check() } returns expectedResponse

        val event = mapOf<String, Any?>(
            "routeKey" to "/check",
        )

        val handler = RestHandler("tracer", false, "us-east-1")

        // Act
        val response = handler.handleRequest(event, context)

        // Assert
        response shouldBe expectedResponse

        coVerify(exactly = 1) {
            anyConstructed<Model>().check()
        }
    }

    @Test
    fun `handleRequest returns health check response for GET colon check route`() {
        // Arrange
        mockkConstructor(Model::class)

        val expectedResponse = HealthCheckResponse(
            statusCode = 200,
            timestamp = 1_717_000_000_000L,
            region = "us-east-1",
        )

        coEvery { anyConstructed<Model>().check() } returns expectedResponse

        val event = mapOf<String, Any?>(
            "routeKey" to "GET:/check",
        )

        val handler = RestHandler("tracer", false, "us-east-1")


        // Act
        val response = handler.handleRequest(event, context)

        // Assert
        response shouldBe expectedResponse

        coVerify(exactly = 1) {
            anyConstructed<Model>().check()
        }
    }

    @Test
    fun `handleRequest derives check route from httpMethod and path`() {
        // Arrange
        mockkConstructor(Model::class)

        val expectedResponse = HealthCheckResponse(
            statusCode = 200,
            timestamp = 1_717_000_000_000L,
            region = "us-east-1",
        )

        coEvery { anyConstructed<Model>().check() } returns expectedResponse

        val event = mapOf<String, Any?>(
            "httpMethod" to "GET",
            "path" to "/check",
        )

        val handler = RestHandler("tracer", false, "us-east-1")

        // Act
        val response = handler.handleRequest(event, context)

        // Assert
        response shouldBe expectedResponse

        coVerify(exactly = 1) {
            anyConstructed<Model>().check()
        }
    }

    @Test
    fun `handleRequest derives check route from request context http method and raw path`() {
        // Arrange
        mockkConstructor(Model::class)

        val expectedResponse = HealthCheckResponse(
            statusCode = 200,
            timestamp = 1_717_000_000_000L,
            region = "us-east-1",
        )

        coEvery { anyConstructed<Model>().check() } returns expectedResponse

        val event = mapOf<String, Any?>(
            "requestContext" to mapOf(
                "http" to mapOf(
                    "method" to "GET",
                ),
            ),
            "rawPath" to "/check",
        )

        val handler = RestHandler("tracer", false, "us-east-1")

        // Act
        val response = handler.handleRequest(event, context)

        // Assert
        response shouldBe expectedResponse

        coVerify(exactly = 1) {
            anyConstructed<Model>().check()
        }
    }

    @Test
    fun `handleRequest returns not found for unknown route`() {
        // Arrange
        mockkConstructor(Model::class)

        val event = mapOf<String, Any?>(
            "routeKey" to "GET /unknown",
        )

        val handler = RestHandler("tracer", false, "us-east-1")

        // Act
        val response = handler.handleRequest(event, context)

        // Assert
        response shouldBe mapOf(
            "statusCode" to 404,
            "body" to "Not Found",
        )

        coVerify(exactly = 0) {
            anyConstructed<Model>().check()
        }
    }

    @Test
    fun `handleRequest returns not found when route cannot be resolved`() {
        // Arrange
        mockkConstructor(Model::class)

        val event = emptyMap<String, Any?>()

        val handler = RestHandler("tracer", false, "us-east-1")

        // Act
        val response = handler.handleRequest(event, context)

        // Assert
        response shouldBe mapOf(
            "statusCode" to 404,
            "body" to "Not Found",
        )

        coVerify(exactly = 0) {
            anyConstructed<Model>().check()
        }
    }
}