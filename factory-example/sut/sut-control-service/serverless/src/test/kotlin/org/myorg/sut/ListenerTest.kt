package org.myorg.sut

import com.amazonaws.services.lambda.runtime.events.KinesisEvent
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.params.ParameterizedTest
import org.myorg.sut.testsupport.EventsMicrostoreFake
import org.myorg.sut.testsupport.TestContext
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.nio.charset.StandardCharsets.UTF_8
import java.time.Instant
import kotlin.test.assertEquals

typealias TestEvent = com.amazonaws.services.lambda.runtime.tests.annotations.Event

class ListenerTest {

    private var eventsMicrostore = EventsMicrostoreFake<TrackedUnitEvent>()

    private var kinesisAdapter = MyKinesisAdapter()

    private var listener : Listener? = Listener(eventsMicrostore, kinesisAdapter)

    @BeforeEach
    fun beforeEach() {
        eventsMicrostore.reset()
    }

    @ParameterizedTest
    @TestEvent(value = "events/kinesis_basic.json", type = KinesisEvent::class)
    fun testBasicKinesisEvent(event: KinesisEvent) {
        val context = TestContext()

        event.records[0].kinesis.setData(encodePayload(ship1Event1))
        event.records[1].kinesis.setData(encodePayload(ship1Event2))

        listener!!.handleRequest(event, context)

        assertEquals(2,eventsMicrostore.getEvents().size)
        val uow1 = eventsMicrostore.getEvents()[0]
        val uow2 = eventsMicrostore.getEvents()[1]

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

    val ship1Event1 = TrackedUnitEvent().apply {
        id = "SHIP-001-2025"
        entity = shipment1
        timestamp = Instant.now().toEpochMilli() / 1000
        location = "Springfield, IL"
        type = "SHIPMENT_CREATED"
        result = "SUCCESS"

    }

    val ship1Event2 = TrackedUnitEvent().apply {
        id = "SHIP-002-2025"
        entity = shipment1
        timestamp = Instant.now().toEpochMilli() / 1000
        location = "Chicago, IL"
        type = "SHIPMENT_RECEIVED_AT_WAREHOUSE"
        result = "SUCCESS"
    }

}