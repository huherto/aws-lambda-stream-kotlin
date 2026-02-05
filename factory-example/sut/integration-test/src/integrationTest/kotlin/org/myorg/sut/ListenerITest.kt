package org.myorg.sut

import aws.sdk.kotlin.runtime.auth.credentials.StaticCredentialsProvider
import aws.sdk.kotlin.services.eventbridge.EventBridgeClient
import aws.sdk.kotlin.services.eventbridge.model.PutEventsRequest
import aws.sdk.kotlin.services.eventbridge.model.PutEventsRequestEntry
import aws.smithy.kotlin.runtime.net.url.Url
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test

// Components tested.
//   - Send event to event bridge. sut-event-hub-local-bus.
//   - Event bridge will send event to kinesis stream. sut-event-hub-local-s1
//   - sut-control-service-local-listener will read event from the kinesis stream.
//   - sut-control-service-local-listener will insert the event in DynamoDB. sut-control-service-local-events
//@Testcontainers
class ListenerITest {

    @Test
    fun sendEvents() {

        runBlocking {
            val eventBridgeClient = createEventBridgeClient(Url.parse("http://localhost:4566"))
            eventBridgeClient.use { eventBridgeClient ->
                val event = createShipmentCreatedEvent(createTrackedUnit())

                val res = eventBridgeClient.putEvents(PutEventsRequest{
                    entries = listOf(
                        PutEventsRequestEntry {
                            eventBusName = "sut-event-hub-local-bus"
                            detail = event.encoded()
                            detailType = "my-event"
                            source = "integration-test"
                        }
                    )
                })
                println("failedEntryCount=${res.failedEntryCount}")
                for (entry in res.entries!!) {
                    println("eventID=${entry.eventId}")
                }
            }
        }
    }

    fun createEventBridgeClient(endpointUrl : Url): EventBridgeClient {

        return EventBridgeClient {
            this.region = "us-east-1"
            this.endpointUrl = endpointUrl
            credentialsProvider =
                StaticCredentialsProvider {
                    this.accessKeyId = "test"
                    this.secretAccessKey = "test"
                }
        }
    }

    private fun createTrackedUnit() = TrackedUnit().apply {
        id = "unit-123"
        senderFullName = "John Doe"
        returnAddress = TrackedUnit.Address("123 Main St", "Atlanta", "GA", "30303")
        destinationAddress = TrackedUnit.Address("456 Oak St", "Miami", "FL", "33101")
        trackingNumber = "TRK123456789"
        weight = 10.5
        dimensions = TrackedUnit.PackageDimensions(12.0, 8.0, 6.0)
    }

    private fun createShipmentCreatedEvent(trackedUnit : TrackedUnit) = ShipmentCreatedEvent().apply {
        id = "ship-123"
        partitionKey = "ship-123"
        timestamp = System.currentTimeMillis()
        location = "Atlanta Hub"
        entity = trackedUnit
    }
}