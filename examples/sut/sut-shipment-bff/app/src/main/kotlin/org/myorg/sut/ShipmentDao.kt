package org.myorg.sut

import aws.sdk.kotlin.services.dynamodb.DynamoDbClient
import aws.sdk.kotlin.services.dynamodb.model.AttributeValue
import aws.sdk.kotlin.services.dynamodb.model.GetItemRequest
import aws.sdk.kotlin.services.dynamodb.model.UpdateItemRequest
import io.github.huherto.awsLambdaStream.asJson
import io.github.huherto.awsLambdaStream.sinks.DynamoDbUpdateValue.DbSet
import io.github.huherto.awsLambdaStream.sinks.updateExpression
import io.github.huherto.awsLambdaStream.utils.nullableN
import io.github.huherto.awsLambdaStream.utils.nullableS

class ShipmentDao(
    private val dynamoDbClient: DynamoDbClient, 
    private val tableName: String) {

    fun tableName() = tableName

    suspend fun getShipmentById(shipmentId: String) : Shipment? {
        val response = dynamoDbClient.getItem(
            GetItemRequest {
                tableName = tableName()
                key = mapOf(
                    "pk" to AttributeValue.S(shipmentId),
                    "sk" to AttributeValue.S("SHIPMENT")
                )
            }
        )

        val item = response.item ?: return null
        return itemMapToShipment(item)
    }

    suspend fun saveShipment(entity: Shipment) {
        val request = createUpdateItemRequest(entity) ?: return
        dynamoDbClient.updateItem(request)
    }

    private fun createUpdateItemRequest(entity: Shipment): UpdateItemRequest? {
        val entityId = entity.id ?: return null

        return UpdateItemRequest {

            tableName = tableName()

            key = mapOf(
                "pk" to AttributeValue.S(entityId),
                "sk" to AttributeValue.S("SHIPMENT")
            )

            val map = mutableMapOf(
                "senderFullName" to DbSet(nullableS(entity.senderFullName)),
                "trackingNumber" to DbSet(nullableS(entity.trackingNumber)),
                "returnAddress" to DbSet(nullableS(entity.returnAddress?.asJson())),
                "destinationAddress" to DbSet(nullableS(entity.destinationAddress?.asJson())),
                "weight" to DbSet(nullableN(entity.weight)),
            )
            entity.dimensions?.let {
                map["dimensions.length"] = DbSet(nullableN(it.length))
                map["dimensions.width"] = DbSet(nullableN(it.width))
                map["dimensions.height"] = DbSet(nullableN(it.height))
            }

            val ue = updateExpression(map)

            updateExpression = ue.updateExpression
            expressionAttributeNames = ue.expressionAttributeNames
            expressionAttributeValues = ue.expressionAttributeValues
        }
    }

}