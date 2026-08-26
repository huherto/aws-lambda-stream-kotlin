package org.myorg.sut.facades

import aws.sdk.kotlin.runtime.auth.credentials.StaticCredentialsProvider
import aws.sdk.kotlin.services.lambda.LambdaClient
import aws.sdk.kotlin.services.lambda.model.InvocationType
import aws.sdk.kotlin.services.lambda.model.InvokeRequest
import aws.smithy.kotlin.runtime.net.url.Url
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyRequestEvent
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent
import io.github.huherto.awsLambdaStream.toJsonElement
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.*
import org.myorg.sut.TrackedUnit

class RestApiFacade {
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

    private val lambdaFunctionName = "sut-shipment-bff-local-restapi"

    suspend fun post(shipment: TrackedUnit): APIGatewayProxyResponseEvent {
        val request = APIGatewayProxyRequestEvent()
            .withHttpMethod("POST")
            .withResource("/shipment")
            .withPath("/shipment")
            .withBody(shipment.toJson())
            .withIsBase64Encoded(false)

        val response = lambdaClient.invoke(InvokeRequest {
            functionName = lambdaFunctionName
            invocationType = InvocationType.RequestResponse
            payload = request.toJsonElement().toString().encodeToByteArray()
        })

        val payload = response.payload
            ?.decodeToString()
            ?: error("RestApi Lambda returned an empty payload")

        if (response.functionError != null) {
            error("RestApi Lambda invocation failed: ${response.functionError}. Payload: $payload")
        }

        return json.parseToJsonElement(payload).jsonObject.toAPIGatewayProxyResponseEvent()
    }

    fun get(shipmentId: String): TrackedUnit? = runBlocking {
        val request = APIGatewayProxyRequestEvent()
            .withHttpMethod("GET")
            .withResource("/shipment/{id}")
            .withPath("/shipment/$shipmentId")
            .withPathParameters(mapOf("id" to shipmentId))
            .withIsBase64Encoded(false)

        val response = lambdaClient.invoke(InvokeRequest {
            functionName = lambdaFunctionName
            invocationType = InvocationType.RequestResponse
            payload = request.toJsonElement().toString().encodeToByteArray()
        })

        val payload = response.payload
            ?.decodeToString()
            ?: error("RestApi Lambda returned an empty payload")

        if (response.functionError != null) {
            error("RestApi Lambda invocation failed: ${response.functionError}. Payload: $payload")
        }

        val apiResponse = json.parseToJsonElement(payload).jsonObject.toAPIGatewayProxyResponseEvent()

        when (apiResponse.statusCode) {
            200 -> json.decodeFromString<TrackedUnit>(apiResponse.body)
            404 -> null
            else -> error("RestApi GET /shipment/$shipmentId failed with status ${apiResponse.statusCode}. Body: ${apiResponse.body}")
        }
    }

    private fun JsonObject.toAPIGatewayProxyResponseEvent(): APIGatewayProxyResponseEvent {
        return APIGatewayProxyResponseEvent().apply {
            statusCode = this@toAPIGatewayProxyResponseEvent["statusCode"]?.jsonPrimitive?.int
            body = this@toAPIGatewayProxyResponseEvent["body"]?.jsonPrimitive?.contentOrNull
            isBase64Encoded = this@toAPIGatewayProxyResponseEvent["isBase64Encoded"]?.jsonPrimitive?.booleanOrNull ?: false
            headers = this@toAPIGatewayProxyResponseEvent["headers"]?.jsonObject?.mapValues { it.value.jsonPrimitive.content }
            multiValueHeaders = this@toAPIGatewayProxyResponseEvent["multiValueHeaders"]?.jsonObject?.mapValues {
                it.value.jsonArray.map { item -> item.jsonPrimitive.content }
            }
        }
    }
}