package org.myorg.sut

import aws.sdk.kotlin.services.dynamodb.DynamoDbClient
import aws.sdk.kotlin.services.dynamodb.model.PutItemRequest
import aws.sdk.kotlin.services.dynamodb.model.PutItemResponse
import com.amazonaws.services.lambda.runtime.events.KinesisEvent
import io.github.huherto.awsLambdaStream.EnvironmentConfig
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.params.ParameterizedTest
import io.github.huherto.awsLambdaStream.testsupport.EventsMicrostoreFake
import io.github.huherto.awsLambdaStream.testsupport.TestContext
import io.mockk.coEvery
import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets.UTF_8
import java.time.Instant
import kotlin.test.assertEquals
import kotlin.test.assertNotNull

typealias TestEvent = com.amazonaws.services.lambda.runtime.tests.annotations.Event

class ListenerTest {

    private var eventsMicrostore = EventsMicrostoreFake()

    private var kinesisAdapter = MyKinesisAdapter()

    private val listener : Listener by lazy {
        val envConfig = mockk<EnvironmentConfig>()
        every { envConfig.awsRegion() } returns "us-east-1"
        every { envConfig.tableName() } returns "events"
        val dynamoDbClient = mockk<DynamoDbClient>()
        val putRequestSlot = slot<PutItemRequest>()

        coEvery { dynamoDbClient.putItem(capture(putRequestSlot)) } coAnswers { mockk<PutItemResponse>() }
        coEvery { dynamoDbClient.putItem(any()) } coAnswers { mockk<PutItemResponse>() }
        Listener(eventsMicrostore, kinesisAdapter, envConfig, dynamoDbClient)
    } 

    @BeforeEach
    fun beforeEach() {
        eventsMicrostore.reset()
    }

    @ParameterizedTest
    @TestEvent(value = "events/kinesis_basic.json", type = KinesisEvent::class)
    fun testBasicKinesisEvent(event: KinesisEvent) {
        val context = TestContext()

        event.records[0].kinesis.data = encodePayload(ship1Event1)
        event.records[1].kinesis.data = encodePayload(ship1Event2)

        listener.handleRequest(event, context)



        assertEquals(2,eventsMicrostore.getEvents().size)
        val uow1 = eventsMicrostore.getEvents()["SHIP-001-2025"]
        val uow2 = eventsMicrostore.getEvents()["SHIP-002-2025"]

        assertNotNull(uow1)
        assertNotNull(uow2)
        assertEquals(ship1Event1.toString(), uow1.event.toString())
        assertEquals(ship1Event2.toString(), uow2.event.toString())

    }

    private fun encodePayload(payload: TrackedUnitEvent): ByteBuffer? {
        return UTF_8.encode(sutJson.encodeToString(payload))
    }

    val shipment1 = TrackedUnit().apply {
        id = "SHIP-001"
        senderFullName = "Alice Sender"
        returnAddress = TrackedUnit.Address(
            street = "123 Main St",
            city = "Springfield",
            state = "IL",
            zip = "62701"
        )
        destinationAddress = TrackedUnit.Address(
            street = "987 Market Ave",
            city = "Chicago",
            state = "IL",
            zip = "60601"
        )
        trackingNumber = "TRACK-001-2025"
        weight = 1.5
        dimensions = TrackedUnit.PackageDimensions(
            length = 30.0,
            width = 20.0,
            height = 10.0
        )
    }

    val ship1Event1 = ShipmentCreatedEvent().apply {
        id = "SHIP-001-2025"
        entity = shipment1
        timestamp = Instant.now().toEpochMilli() / 1000
        location = "Springfield, IL"
        result = "SUCCESS"

    }

    val ship1Event2 = ShipmentDeliveredEvent().apply {
        id = "SHIP-002-2025"
        entity = shipment1
        timestamp = Instant.now().toEpochMilli() / 1000
        location = "Chicago, IL"
        result = "SUCCESS"
    }

}