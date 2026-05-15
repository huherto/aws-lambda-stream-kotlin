package org.myorg.sut

import com.amazonaws.services.lambda.runtime.Context
import com.amazonaws.services.lambda.runtime.RequestHandler
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyRequestEvent
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent
import io.github.huherto.awsLambdaStream.asJson
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.SerializationException
import kotlinx.serialization.json.Json.Default.decodeFromString
import mu.KotlinLogging

class RestApi(private val container: RestApiContainer = RestApiContainer.build()) : RequestHandler<APIGatewayProxyRequestEvent, APIGatewayProxyResponseEvent> {

    private val logger = KotlinLogging.logger {  }

    private val shipmentDao: ShipmentDao = container.shipmentDao

    override fun handleRequest(
        input: APIGatewayProxyRequestEvent,
        context: Context
    ): APIGatewayProxyResponseEvent = runBlocking {
        logger.warn("Received input: $input")

        if (input.httpMethod == "GET" && input.resource == "/shipment/{id}") {
            val shipmentId = input.pathParameters?.get("id")
            return@runBlocking getShipmentById(shipmentId)
        }

        if (input.httpMethod == "POST" && input.resource == "/shipment") {
            return@runBlocking saveShipment(input.body)
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

        val shipment = shipmentDao.getShipmentById(shipmentId) ?: return shipmentNotFound()

        return jsonResponse(
            statusCode = 200,
            body = shipment.asJson()
        )

    }

    private suspend fun saveShipment(body: String?): APIGatewayProxyResponseEvent {
        if (body.isNullOrBlank()) {
            return jsonResponse(
                statusCode = 400,
                body = """{"message":"Missing request body"}"""
            )
        }

        val shipment = try {
            decodeFromString<Shipment>(body)
        } catch (ex: SerializationException) {
            logger.warn(ex) { "Invalid shipment request body" }
            return jsonResponse(
                statusCode = 400,
                body = """{"message":"Invalid shipment request body"}"""
            )
        } catch (ex: IllegalArgumentException) {
            logger.warn(ex) { "Invalid shipment request body" }
            return jsonResponse(
                statusCode = 400,
                body = """{"message":"Invalid shipment request body"}"""
            )
        }

        if (shipment.id.isNullOrBlank()) {
            return jsonResponse(
                statusCode = 400,
                body = """{"message":"Missing shipment id"}"""
            )
        }

        shipmentDao.saveShipment(shipment)

        return jsonResponse(
            statusCode = 201,
            body = shipment.asJson()
        )
    }

    private fun shipmentNotFound(): APIGatewayProxyResponseEvent = jsonResponse(
        statusCode = 404,
        body = """{"message":"Shipment not found"}"""
    )

    private fun jsonResponse(
        statusCode: Int,
        body: String
    ): APIGatewayProxyResponseEvent =
        APIGatewayProxyResponseEvent()
            .withStatusCode(statusCode)
            .withHeaders(mapOf("Content-Type" to "application/json"))
            .withBody(body)

}