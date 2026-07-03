package org.myorg.sut.facades

import aws.sdk.kotlin.services.eventbridge.EventBridgeClient
import aws.sdk.kotlin.services.eventbridge.model.PutEventsRequest
import aws.sdk.kotlin.services.eventbridge.model.PutEventsRequestEntry
import io.github.huherto.awsLambdaStream.Event
import io.kotest.matchers.shouldBe

class EventBridgeFacade(
    private val config: AwsLocalConfig = AwsLocalConfig(),
    private val eventBusName: String = "sut-event-hub-local-bus",
) {
    val client: EventBridgeClient by lazy {
        EventBridgeClient {
            region = config.region
            endpointUrl = config.endpointUrl
            credentialsProvider = config.credentialsProvider()
        }
    }

    suspend fun putEvents(vararg events: Event) {
        val entries = events.map { event ->
            PutEventsRequestEntry {
                eventBusName = this@EventBridgeFacade.eventBusName
                detail = event.encoded()
                detailType = event.eventType()
                source = "integration-test"
            }
        }

        val res = client.putEvents(PutEventsRequest {
            this.entries = entries
        })

        res.failedEntryCount shouldBe 0
    }

    fun close() {
        client.close()
    }
}