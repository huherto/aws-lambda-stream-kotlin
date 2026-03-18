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
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import kotlin.random.Random
import kotlin.time.ExperimentalTime

// Components tested.
//   - Send event to event bridge. sut-event-hub-local-bus.
//   - Event bridge will send event to kinesis stream. sut-event-hub-local-s1
//   - sut-control-service-local-listener will read event from the kinesis stream.
//   - sut-control-service-local-listener will insert the event in DynamoDB. sut-control-service-local-events
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ListenerITest {

    fun endPointUrl() = Url.parse("http://localhost:4566")

    @OptIn(ExperimentalTime::class)
    @Test
    fun sendEvents() = runBlocking{

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
        assertEquals(0, res.failedEntryCount)
        val collectedEvent = findEventByPK( event.id!!)
        assertNotNull(collectedEvent)
        assertEquals(event.id, collectedEvent?.get("pk")?.asS())
        assertEquals("EVENT", collectedEvent?.get("sk")?.asS())
        assertEquals("EVENT", collectedEvent?.get("discriminator")?.asS())
        assertEquals(event.id, collectedEvent?.get("data")?.asS())
        assertNotNull(collectedEvent?.get("event")?.asS())

        val timeStamp = collectedEvent?.get("timestamp")?.asN()?.toLong()?:0
        assertTrue((event.timestamp?:0) - timeStamp < 1000)


        val correlEvent = findEventByPK(event.entity?.id!!)
        assertNotNull(correlEvent)
        assertEquals(event.entity?.id, correlEvent?.get("pk")?.asS())
        assertEquals(event.id, correlEvent?.get("sk")?.asS())
        assertEquals("CORREL", correlEvent?.get("discriminator")?.asS())
        //assertEquals(event.id, correlEvent?.get("data")?.asS())
        assertNotNull(correlEvent?.get("sequenceNumber")?.asS())
        assertNotNull(correlEvent?.get("event")?.asS())

        val ttl = collectedEvent?.get("ttl")?.asN()?.toLong()
        assertNotNull(ttl)
        assertTrue((ttl?:0) > 0)
        assertTrue((ttl?:0) > 1742326911) // A date in 2025
        assertTrue((ttl?:0) < 1900093311) // A date in 2030

    }

    @Test
    fun sendPoisonPillEvent() = runBlocking {

        val event = createPoisonPillEvent(createTrackedUnit())

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
        assertEquals(0, res.failedEntryCount)
    }

    private suspend fun findEventByPK(pk : String): Map<String, AttributeValue>? {

        val startTime = System.currentTimeMillis()
        while (true) {
            if (System.currentTimeMillis() - startTime > 10000) {
                throw RuntimeException("Timed out waiting for event to be inserted.")
            }
            val response = dynamoDbClient.query(QueryRequest {
                tableName = "sut-control-service-local-events"
                keyConditionExpression = "pk = :pk"
                expressionAttributeValues = mapOf(":pk" to AttributeValue.S(pk))
            })
            // println("Items found: ${response.items?.size}")
            response.items?.forEach { item ->
                // println("Found item: $item")
            }
            if (response.items?.isNotEmpty() == true) {
                return response.items?.first()
            }
            Thread.sleep(1000)
        }
    }

    private val eventBridgeClient: EventBridgeClient by lazy {
        EventBridgeClient {
            this.region = "us-east-1"
            this.endpointUrl = endPointUrl()
            credentialsProvider =
                StaticCredentialsProvider {
                    this.accessKeyId = "test"
                    this.secretAccessKey = "test"
                }
        }
    }

    private val dynamoDbClient:  DynamoDbClient by lazy {
        DynamoDbClient {
            this.region = "us-east-1"
            this.endpointUrl = endPointUrl()
            credentialsProvider =
                StaticCredentialsProvider {
                    this.accessKeyId = "test"
                    this.secretAccessKey = "test"
                }
        }
    }

    @AfterAll
    fun tearDownAll() {
        eventBridgeClient.close()
        dynamoDbClient.close()
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

    private fun createPoisonPillEvent(trackedUnit : TrackedUnit) = ShipmentCreatedEvent().apply {
        id = "poison-"+generateRandomNumber()
        partitionKey = id
        timestamp = System.currentTimeMillis()
        location = "poison-pill"
        entity = trackedUnit
    }
}