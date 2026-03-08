package org.myorg.sut

import aws.sdk.kotlin.runtime.auth.credentials.StaticCredentialsProvider
import aws.sdk.kotlin.services.dynamodb.DynamoDbClient
import aws.sdk.kotlin.services.dynamodb.model.AttributeValue
import aws.sdk.kotlin.services.dynamodb.model.QueryRequest
import aws.sdk.kotlin.services.eventbridge.EventBridgeClient
import aws.sdk.kotlin.services.eventbridge.model.PutEventsRequest
import aws.sdk.kotlin.services.eventbridge.model.PutEventsRequestEntry
import aws.smithy.kotlin.runtime.net.url.Url
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import kotlin.random.Random

// Components tested.
//   - Send event to event bridge. sut-event-hub-local-bus.
//   - Event bridge will send event to kinesis stream. sut-event-hub-local-s1
//   - sut-control-service-local-listener will read event from the kinesis stream.
//   - sut-control-service-local-listener will insert the event in DynamoDB. sut-control-service-local-events
//@Testcontainers
class ListenerITest {

    fun endPointUrl() = Url.parse("http://localhost:4566")

    @Test
    fun sendEvents() = runBlocking{

        val eventBridgeClient = createEventBridgeClient()
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
            val dynamoDbClient = createDynamoDbClient()
            dynamoDbClient.use { dynamoDbClient ->
                val savedEvent = findEvent(dynamoDbClient, event.id!!)
                assert(savedEvent != null)
            }
        }
    }

    private suspend fun findEvent(dynamoDbClient: DynamoDbClient, eventId : String): Map<String, AttributeValue>? {
        println("Looking for event with id: $eventId")
        val startTime = System.currentTimeMillis()
        while (true) {
            if (System.currentTimeMillis() - startTime > 10000) {
                throw RuntimeException("Timed out waiting for event to be inserted.")
            }
            val response = dynamoDbClient.query(QueryRequest {
                tableName = "sut-control-service-local-events"
                keyConditionExpression = "pk = :pk"
                expressionAttributeValues = mapOf(":pk" to AttributeValue.S(eventId))
            })
            println("Items found: ${response.items?.size}")
            response.items?.forEach { item ->
                println("Found item: $item")
            }
            if (response.items?.isNotEmpty() == true) {
                return response.items?.first()
            }
            Thread.sleep(1000)
        }
    }

    fun createEventBridgeClient(): EventBridgeClient {

        return EventBridgeClient {
            this.region = "us-east-1"
            this.endpointUrl = endPointUrl()
            credentialsProvider =
                StaticCredentialsProvider {
                    this.accessKeyId = "test"
                    this.secretAccessKey = "test"
                }
        }
    }

    fun createDynamoDbClient(): DynamoDbClient {
        return DynamoDbClient {
            this.region = "us-east-1"
            this.endpointUrl = endPointUrl()
            credentialsProvider =
                StaticCredentialsProvider {
                    this.accessKeyId = "test"
                    this.secretAccessKey = "test"
                }
        }
    }

    private fun createTrackedUnit() = TrackedUnit().apply {
        id = "unit-"+generateRandomNumber()
        senderFullName = "John Doe"
        returnAddress = TrackedUnit.Address("123 Main St", "Atlanta", "GA", "30303")
        destinationAddress = TrackedUnit.Address("456 Oak St", "Miami", "FL", "33101")
        trackingNumber = "TRK123456789"
        weight = 10.5
        dimensions = TrackedUnit.PackageDimensions(12.0, 8.0, 6.0)
    }

    private fun generateRandomNumber() : String {
        val rand = Random.Default
        return String.format("%04d", rand.nextLong(10000))
    }

    private fun createShipmentCreatedEvent(trackedUnit : TrackedUnit) = ShipmentCreatedEvent().apply {
        id = "ship-"+generateRandomNumber()
        partitionKey = id
        timestamp = System.currentTimeMillis()
        location = "Atlanta Hub"
        entity = trackedUnit
    }
}