package org.myorg.sut.facades

import aws.sdk.kotlin.services.eventbridge.EventBridgeClient
import aws.sdk.kotlin.services.eventbridge.model.PutEventsRequest
import aws.sdk.kotlin.services.eventbridge.model.PutEventsRequestEntry
import io.github.huherto.awsLambdaStream.Event
import io.github.huherto.awsLambdaStream.faults.FaultEvent
import io.kotest.matchers.shouldBe
import kotlinx.serialization.json.Json

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

    suspend fun putEvents(vararg events: Any) {
        val entries = events.map { event ->
            PutEventsRequestEntry {
                eventBusName = this@EventBridgeFacade.eventBusName
                source = "integration-test"
                when (event) {
                    is Event -> {
                        detail = event.encoded()
                        detailType = event.eventType()
                    }
                    is FaultEvent -> {
                        detail = Json.encodeToString(event)
                        detailType = event.type
                    }
                    else -> error("Unsupported event type: ${event::class}")
                }
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