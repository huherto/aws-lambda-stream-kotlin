package org.myorg.sut

import aws.sdk.kotlin.runtime.auth.credentials.StaticCredentialsProvider
import aws.sdk.kotlin.services.lambda.LambdaClient
import aws.sdk.kotlin.services.lambda.model.InvocationType
import aws.sdk.kotlin.services.lambda.model.InvokeRequest
import aws.smithy.kotlin.runtime.net.url.Url
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyRequestEvent
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import io.github.huherto.awsLambdaStream.asJson

class RestApiFacade {
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

    suspend fun post(shipment: TrackedUnit): APIGatewayProxyResponseEvent {
        val request = APIGatewayProxyRequestEvent()
            .withHttpMethod("POST")
            .withResource("/shipment")
            .withPath("/shipment")
            .withBody(shipment.asJson())
            .withIsBase64Encoded(false)

        val response = lambdaClient.invoke(InvokeRequest {
            functionName = "sut-shipment-bff-local-restapi"
            invocationType = InvocationType.RequestResponse
            payload = objectMapper.writeValueAsBytes(request)
        })

        val payload = response.payload
            ?.decodeToString()
            ?: error("RestApi Lambda returned an empty payload")

        if (response.functionError != null) {
            error("RestApi Lambda invocation failed: ${response.functionError}. Payload: $payload")
        }

        return objectMapper.readValue(payload, APIGatewayProxyResponseEvent::class.java)
    }
}
