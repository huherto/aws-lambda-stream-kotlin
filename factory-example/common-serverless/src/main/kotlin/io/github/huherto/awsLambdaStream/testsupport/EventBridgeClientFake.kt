package io.github.huherto.awsLambdaStream.testsupport

import aws.sdk.kotlin.services.eventbridge.EventBridgeClient
import aws.sdk.kotlin.services.eventbridge.model.PutEventsRequest
import aws.sdk.kotlin.services.eventbridge.model.PutEventsResponse

class EventBridgeClientFake(delegate: EventBridgeClient) : EventBridgeClient by delegate {

    val putEventsRequests = mutableListOf<PutEventsRequest>()

    override suspend fun putEvents(request: PutEventsRequest): PutEventsResponse {
            putEventsRequests.add(request)
        return PutEventsResponse.Companion {}
    }

    fun reset() {
        putEventsRequests.clear()
    }
}