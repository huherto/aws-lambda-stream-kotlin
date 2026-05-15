package org.myorg.sut

import aws.sdk.kotlin.services.dynamodb.DynamoDbClient
import aws.sdk.kotlin.services.dynamodb.model.AttributeValue
import aws.sdk.kotlin.services.dynamodb.model.GetItemRequest
import kotlinx.serialization.json.Json.Default.decodeFromString

typealias Shipment = TrackedUnit

class ShipmentDao(
    private val dynamoDbClient: DynamoDbClient, 
    private val tableName: String) {

    suspend fun getShipmentById(shipmentId: String) : Shipment? {
        val response = dynamoDbClient.getItem(
            GetItemRequest {
                tableName = this@ShipmentDao.tableName
                key = mapOf(
                    "pk" to AttributeValue.S(shipmentId),
                    "sk" to AttributeValue.S("SHIPMENT")
                )
            }
        )

        val item = response.item ?: return null
        return mapItemToShipment(item)
    }

    private fun mapItemToShipment(item: Map<String, AttributeValue>): Shipment {
        val shipment = Shipment().apply {
            id = item.getS("pk")
            senderFullName = item.getS("senderFullName")
            returnAddress = item.getAddress("returnAddress")
            destinationAddress = item.getAddress("destinationAddress")
            trackingNumber = item.getS("trackingNumber")
            weight = item.getDouble("weight")
            dimensions = item.getPackageDimensions("dimensions")
        }
        return shipment
    }

    private fun Map<String, AttributeValue>.getS(fieldName: String) : String? {
        return this.get(fieldName)?.asS()
    }

    private fun Map<String, AttributeValue>.getAddress(fieldName: String) : TrackedUnit.Address? {
        return this.get(fieldName)?.asS()?.let {
            decodeFromString<TrackedUnit.Address>(it)
        }
    }

    private fun Map<String, AttributeValue>.getDouble(fieldName: String) : Double? {
        return this.get(fieldName)?.asN()?.toDouble()
    }

    private fun Map<String, AttributeValue>.getPackageDimensions(fieldName: String) : TrackedUnit.PackageDimensions? {
        return this.get(fieldName)?.asS()?.let {
            decodeFromString<TrackedUnit.PackageDimensions>(it)
        }
    }
    
}