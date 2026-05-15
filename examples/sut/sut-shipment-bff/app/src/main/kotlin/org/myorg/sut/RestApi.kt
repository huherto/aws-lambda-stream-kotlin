package org.myorg.sut

import aws.sdk.kotlin.services.dynamodb.model.AttributeValue
import aws.sdk.kotlin.services.dynamodb.model.GetItemRequest
import com.amazonaws.services.lambda.runtime.Context
import com.amazonaws.services.lambda.runtime.RequestHandler
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyRequestEvent
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent
import kotlinx.coroutines.runBlocking
import mu.KotlinLogging

class RestApi(private val container: RestApiContainer = RestApiContainer.build()) : RequestHandler<APIGatewayProxyRequestEvent, APIGatewayProxyResponseEvent> {

    private val logger = KotlinLogging.logger {  }

    override fun handleRequest(
        input: APIGatewayProxyRequestEvent,
        context: Context
    ): APIGatewayProxyResponseEvent = runBlocking {
        logger.warn("Received input: $input")

        val shipmentId = input.pathParameters?.get("id")

        if (input.httpMethod == "GET" && input.resource == "/shipmment/{id}") {
            return@runBlocking getShipmentById(shipmentId)
        }

        jsonResponse(
            statusCode = 404,
            body = """{"message":"Resource not found"}"""
        )
    }

    private suspend fun getShipmentById(shipmentId: String?): APIGatewayProxyResponseEvent {
        if (shipmentId.isNullOrBlank()) {
            return jsonResponse(
                statusCode = 400,
                body = """{"message":"Missing shipment id"}"""
            )
        }

        val response = container.dynamoDBClient.getItem(
            GetItemRequest {
                tableName = container.tableName
                key = mapOf(
                    "pk" to AttributeValue.S(shipmentId),
                    "sk" to AttributeValue.S("SHIPMENT")
                )
            }
        )

        val item = response.item

        if (item == null || item.isEmpty()) {
            return jsonResponse(
                statusCode = 404,
                body = """{"message":"Shipment not found"}"""
            )
        }

        return jsonResponse(
            statusCode = 200,
            body = shipmentJson(item)
        )

    }

    private fun shipmentJson(item: Map<String, AttributeValue>): String {
        val id = item["pk"]?.asS()
        val senderFullName = item["senderFullName"]?.asS()
        val trackingNumber = item["trackingNumber"]?.asS()
        val weight = item["weight"]?.asN()

        return """
            {
              "id": ${id.toJsonString()},
              "senderFullName": ${senderFullName.toJsonString()},
              "trackingNumber": ${trackingNumber.toJsonString()},
              "weight": ${weight.toJsonNumber()}
            }
        """.trimIndent()
    }

    private fun jsonResponse(
        statusCode: Int,
        body: String
    ): APIGatewayProxyResponseEvent =
        APIGatewayProxyResponseEvent()
            .withStatusCode(statusCode)
            .withHeaders(mapOf("Content-Type" to "application/json"))
            .withBody(body)

    private fun String?.toJsonString(): String =
        this?.let { """"${it.replace("\"", "\\\"")}"""" } ?: "null"

    private fun String?.toJsonNumber(): String =
        this ?: "null"
}