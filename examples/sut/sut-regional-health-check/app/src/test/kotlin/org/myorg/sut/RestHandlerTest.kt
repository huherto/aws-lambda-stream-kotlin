package org.myorg.sut

import com.amazonaws.services.lambda.runtime.Context
import io.kotest.matchers.maps.shouldContain
import io.kotest.matchers.nulls.shouldNotBeNull
import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.shouldBeInstanceOf
import io.mockk.mockk
import org.junit.jupiter.api.Test

class RestHandlerTest {

    @Test
    fun `handleRequest returns not found for unknown route`() {
        // Arrange
        val handler = RestHandler(
            entityTable = "entity-table",
            unhealthyFlag = true,
            awsRegion = "us-east-1",
        )
        val event = mapOf<String, Any?>(
            "routeKey" to "GET /unknown",
        )
        val context = mockk<Context>(relaxed = true)

        // Act
        val response = handler.handleRequest(event, context)

        // Assert
        val responseMap = response.shouldBeInstanceOf<Map<String, Any?>>()
        responseMap["statusCode"] shouldBe 404
        responseMap["body"] shouldBe "Not Found"
    }

    @Test
    fun `handleRequest returns check response for GET check route key`() {
        // Arrange
        val handler = RestHandler(
            entityTable = "entity-table",
            unhealthyFlag = true,
            awsRegion = "us-east-1",
        )
        val event = mapOf<String, Any?>(
            "routeKey" to "GET /check",
        )
        val context = mockk<Context>(relaxed = true)

        // Act
        val response = handler.handleRequest(event, context)

        // Assert
        val responseMap = response.shouldBeInstanceOf<Map<String, Any?>>()
        responseMap["statusCode"] shouldBe 503
        responseMap["headers"].shouldNotBeNull()

        val headers = responseMap["headers"].shouldBeInstanceOf<Map<String, String>>()
        headers shouldContain ("Cache-Control" to "no-cache, no-store, must-revalidate")

        val body = responseMap["body"].shouldBeInstanceOf<HealthCheckResponse>()
        body.statusCode shouldBe 503
        body.region shouldBe "us-east-1"
        body.incomplete shouldBe null
        body.elapsed shouldBe null
        body.get shouldBe null
        body.save shouldBe null
    }

    @Test
    fun `handleRequest returns check response for legacy check path`() {
        // Arrange
        val handler = RestHandler(
            entityTable = "entity-table",
            unhealthyFlag = true,
            awsRegion = "us-east-1",
        )
        val event = mapOf<String, Any?>(
            "routeKey" to "/check",
        )
        val context = mockk<Context>(relaxed = true)

        // Act
        val response = handler.handleRequest(event, context)

        // Assert
        val responseMap = response.shouldBeInstanceOf<Map<String, Any?>>()
        responseMap["statusCode"] shouldBe 503

        val body = responseMap["body"].shouldBeInstanceOf<HealthCheckResponse>()
        body.statusCode shouldBe 503
        body.region shouldBe "us-east-1"
    }

    @Test
    fun `handleRequest returns check response for colon separated check route`() {
        // Arrange
        val handler = RestHandler(
            entityTable = "entity-table",
            unhealthyFlag = true,
            awsRegion = "us-east-1",
        )
        val event = mapOf<String, Any?>(
            "routeKey" to "GET:/check",
        )
        val context = mockk<Context>(relaxed = true)

        // Act
        val response = handler.handleRequest(event, context)

        // Assert
        val responseMap = response.shouldBeInstanceOf<Map<String, Any?>>()
        responseMap["statusCode"] shouldBe 503

        val body = responseMap["body"].shouldBeInstanceOf<HealthCheckResponse>()
        body.statusCode shouldBe 503
        body.region shouldBe "us-east-1"
    }

    @Test
    fun `handleRequest derives route from http method and path`() {
        // Arrange
        val handler = RestHandler(
            entityTable = "entity-table",
            unhealthyFlag = true,
            awsRegion = "us-east-1",
        )
        val event = mapOf<String, Any?>(
            "httpMethod" to "GET",
            "path" to "/check",
        )
        val context = mockk<Context>(relaxed = true)

        // Act
        val response = handler.handleRequest(event, context)

        // Assert
        val responseMap = response.shouldBeInstanceOf<Map<String, Any?>>()
        responseMap["statusCode"] shouldBe 503

        val body = responseMap["body"].shouldBeInstanceOf<HealthCheckResponse>()
        body.statusCode shouldBe 503
        body.region shouldBe "us-east-1"
    }

    @Test
    fun `handleRequest derives route from request context http method and raw path`() {
        // Arrange
        val handler = RestHandler(
            entityTable = "entity-table",
            unhealthyFlag = true,
            awsRegion = "us-east-1",
        )
        val event = mapOf<String, Any?>(
            "requestContext" to mapOf(
                "http" to mapOf(
                    "method" to "GET",
                ),
            ),
            "rawPath" to "/check",
        )
        val context = mockk<Context>(relaxed = true)

        // Act
        val response = handler.handleRequest(event, context)

        // Assert
        val responseMap = response.shouldBeInstanceOf<Map<String, Any?>>()
        responseMap["statusCode"] shouldBe 503

        val body = responseMap["body"].shouldBeInstanceOf<HealthCheckResponse>()
        body.statusCode shouldBe 503
        body.region shouldBe "us-east-1"
    }
}