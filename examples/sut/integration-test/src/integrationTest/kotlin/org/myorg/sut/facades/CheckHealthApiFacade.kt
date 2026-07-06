package org.myorg.sut.facades

import aws.sdk.kotlin.runtime.auth.credentials.StaticCredentialsProvider
import aws.sdk.kotlin.services.lambda.LambdaClient
import aws.sdk.kotlin.services.lambda.model.InvocationType
import aws.sdk.kotlin.services.lambda.model.InvokeRequest
import aws.smithy.kotlin.runtime.net.url.Url
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import kotlinx.coroutines.runBlocking

data class Tracer (
    val awsRegion: String,
    val roundedTimestamp: Long,
    val timestamp: Long,
    val ttl: Long,
    val status: String,
)

data class HealthCheckResponse(
    val statusCode: Int,
    val timestamp: Long,
    val region: String?,
    val incomplete: Boolean? = null,
    val elapsed: Double? = null,
    val tracers: List<Tracer>? = null,
    val saveResponse: String? = null,
)

class CheckHealthApiFacade {
    private val objectMapper = jacksonObjectMapper()
        .disable(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES)

    private val lambdaClient: LambdaClient by lazy {
        LambdaClient {
            region = "us-east-1"
            endpointUrl = Url.parse("http://localhost:4566")
            credentialsProvider = StaticCredentialsProvider {
                accessKeyId = "test"
                secretAccessKey = "test"
            }
        }
    }

    private val lambdaFunctionName = "sut-regional-health-check-local-checkHealthApi"

    fun check(): HealthCheckResponse = runBlocking {
        val request = mapOf<String, Any?>(
            "routeKey" to "GET /check",
            "httpMethod" to "GET",
            "path" to "/check",
        )

        val response = lambdaClient.invoke(InvokeRequest {
            functionName = lambdaFunctionName
            invocationType = InvocationType.RequestResponse
            payload = objectMapper.writeValueAsBytes(request)
        })

        val payload = response.payload
            ?.decodeToString()
            ?: error("CheckHealthApi Lambda returned an empty payload")

        if (response.functionError != null) {
            error("CheckHealthApi Lambda invocation failed: ${response.functionError}. Payload: $payload")
        }

        val apiResponse = objectMapper.readValue(payload, Map::class.java)
        val statusCode = apiResponse["statusCode"] as? Int
            ?: error("CheckHealthApi response did not contain a statusCode. Payload: $payload")

        val body = apiResponse["body"]
            ?: error("CheckHealthApi response did not contain a body. Payload: $payload")

        val healthCheckResponse = objectMapper.convertValue(body, HealthCheckResponse::class.java)

        if (statusCode != healthCheckResponse.statusCode) {
            error("CheckHealthApi returned mismatched status codes. Response status: $statusCode. Body status: ${healthCheckResponse.statusCode}")
        }

        healthCheckResponse
    }
}