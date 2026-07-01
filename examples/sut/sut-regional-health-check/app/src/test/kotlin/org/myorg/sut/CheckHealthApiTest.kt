package org.myorg.sut

import aws.sdk.kotlin.services.dynamodb.DynamoDbClient
import com.amazonaws.services.lambda.runtime.Context
import io.github.huherto.awsLambdaStream.EnvironmentConfig
import io.kotest.matchers.maps.shouldContain
import io.kotest.matchers.nulls.shouldNotBeNull
import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.shouldBeInstanceOf
import io.mockk.coEvery
import io.mockk.mockk
import io.mockk.spyk
import org.junit.jupiter.api.Test

class CheckHealthApiTest {

    fun createContainer(unhealthyFlag: Boolean) : CheckHealthApiContainer {
        val envConfig = spyk(EnvironmentConfig())
        coEvery { envConfig.awsRegion() } returns "us-east-1"
        coEvery { envConfig.entityTableName() } returns "tracer-table-name"
        coEvery { envConfig.unhealthy() } returns unhealthyFlag

        return CheckHealthApiContainer(envConfig, dynamoDBClient = mockk<DynamoDbClient>())
    }

    @Test
    fun `handleRequest returns not found for unknown route`() {
        // Arrange
        val handler = CheckHealthApi(createContainer(false))

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
        val handler = CheckHealthApi(createContainer(true))
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
        body.tracers shouldBe null
        body.saveResponse shouldBe null
    }

    @Test
    fun `handleRequest returns check response for legacy check path`() {
        // Arrange
        val handler = CheckHealthApi(createContainer(true))
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
        val handler = CheckHealthApi(createContainer(true))
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
        val handler = CheckHealthApi(createContainer(true))
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
        val handler = CheckHealthApi(createContainer(true))
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