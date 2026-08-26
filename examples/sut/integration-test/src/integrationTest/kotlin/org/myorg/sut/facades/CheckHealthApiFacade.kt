package org.myorg.sut.facades

import aws.sdk.kotlin.runtime.auth.credentials.StaticCredentialsProvider
import aws.sdk.kotlin.services.lambda.LambdaClient
import aws.sdk.kotlin.services.lambda.model.InvocationType
import aws.sdk.kotlin.services.lambda.model.InvokeRequest
import aws.smithy.kotlin.runtime.net.url.Url
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.*

@Serializable
data class Tracer (
    val awsRegion: String,
    val roundedTimestamp: Long,
    val timestamp: Long,
    val ttl: Long,
    val status: String,
)

@Serializable
data class HealthCheckResponse(
    val statusCode: Int,
    val timestamp: Long,
    val region: String? = null,
    val incomplete: Boolean? = null,
    val elapsed: Double? = null,
    val tracers: List<Tracer>? = null,
    val saveResponse: String? = null,
)

class CheckHealthApiFacade {
    private val json = Json {
        ignoreUnknownKeys = true
    }

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
        val request = buildJsonObject {
            put("routeKey", "GET /check")
            put("httpMethod", "GET")
            put("path", "/check")
        }

        val response = lambdaClient.invoke(InvokeRequest {
            functionName = lambdaFunctionName
            invocationType = InvocationType.RequestResponse
            payload = request.toString().encodeToByteArray()
        })

        val payload = response.payload
            ?.decodeToString()
            ?: error("CheckHealthApi Lambda returned an empty payload")

        if (response.functionError != null) {
            error("CheckHealthApi Lambda invocation failed: ${response.functionError}. Payload: $payload")
        }

        val apiResponse = json.parseToJsonElement(payload).jsonObject
        val statusCode = apiResponse["statusCode"]?.jsonPrimitive?.int
            ?: error("CheckHealthApi response did not contain a statusCode. Payload: $payload")

        val bodyElement = apiResponse["body"]
            ?: error("CheckHealthApi response did not contain a body. Payload: $payload")

        val healthCheckResponse = if (bodyElement is JsonPrimitive && bodyElement.isString) {
            json.decodeFromString<HealthCheckResponse>(bodyElement.content)
        } else {
            json.decodeFromJsonElement<HealthCheckResponse>(bodyElement)
        }

        if (statusCode != healthCheckResponse.statusCode) {
            error("CheckHealthApi returned mismatched status codes. Response status: $statusCode. Body status: ${healthCheckResponse.statusCode}")
        }

        healthCheckResponse
    }
}