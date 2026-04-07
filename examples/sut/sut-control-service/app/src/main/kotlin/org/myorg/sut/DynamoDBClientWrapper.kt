package org.myorg.sut

import aws.sdk.kotlin.services.dynamodb.DynamoDbClient
import aws.sdk.kotlin.services.dynamodb.model.PutItemRequest
import aws.sdk.kotlin.services.dynamodb.model.PutItemResponse
import mu.KotlinLogging

// Integration tests helper.
// It creates artificial faults when writing to the database.
class DynamoDBClientWrapper(val dynamoDBClient: DynamoDbClient) : DynamoDbClient by dynamoDBClient {

    private val logger = KotlinLogging.logger {  }

    override suspend fun putItem(input: PutItemRequest): PutItemResponse {
        val eventId = input.item?.get("pk")?.asS()
        logger.info { "Checking for poison event: $eventId" }
        if (eventId?.contains("poison") == true) {
            throw RuntimeException("Poison event detected $eventId")
        }
        logger.info { "Putting item: $eventId" }
        return dynamoDBClient.putItem(input)
    }
}