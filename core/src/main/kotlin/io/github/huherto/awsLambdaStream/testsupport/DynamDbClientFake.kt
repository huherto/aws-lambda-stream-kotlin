package io.github.huherto.awsLambdaStream.testsupport

import aws.sdk.kotlin.services.dynamodb.DynamoDbClient
import aws.sdk.kotlin.services.dynamodb.model.PutItemRequest
import aws.sdk.kotlin.services.dynamodb.model.PutItemResponse

class DynamDbClientFake(
    private val delegate: DynamoDbClient
) : DynamoDbClient by delegate {

    // Capture the requests to verify them later in your tests
    val putRequests = mutableMapOf<String?, PutItemRequest>()

    override suspend fun putItem(input: PutItemRequest): PutItemResponse {

        val eventId = input.item?.get("pk")?.asS()
        // Intercept the call and save the request
        putRequests.put(eventId, input)

        input.item?.apply {}

        // Return an empty/successful response without hitting the network
        return PutItemResponse.Companion { }
    }


    fun reset() {
        putRequests.clear()
    }
}