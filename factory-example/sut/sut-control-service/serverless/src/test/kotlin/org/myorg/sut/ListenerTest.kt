package org.myorg.sut

import com.amazonaws.services.lambda.runtime.events.KinesisEvent
import com.amazonaws.services.lambda.runtime.tests.annotations.Event
import kotlinx.serialization.json.Json
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.params.ParameterizedTest
import org.myorg.sut.testsupport.EventsMicrostoreFake
import org.myorg.sut.testsupport.TestContext
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets.UTF_8
import kotlin.test.assertEquals

class ListenerTest {

    class MyKinesisAdapter : KinesisAdapter<TrackedUnit>() {
    }

    private var eventsMicrostore = EventsMicrostoreFake<TrackedUnit>()

    private var kinesisAdapter = MyKinesisAdapter()

    private var listener : Listener? = Listener(eventsMicrostore, kinesisAdapter)

    @BeforeEach
    fun beforeEach() {
        eventsMicrostore.reset();
    }

    @ParameterizedTest
    @Event(value = "events/kinesis_basic.json", type = KinesisEvent::class)
    fun testBasicKinesisEvent(event: KinesisEvent): Unit {
        val context = TestContext()

        event.records[0].kinesis.setData(encodePayload(shipment1))
        event.records[1].kinesis.setData(encodePayload(shipment2))

        listener!!.handleRequest(event, context)

        assertEquals(2,eventsMicrostore.getEvents().size)
        val uow1 = eventsMicrostore.getEvents()[0]
        val uow2 = eventsMicrostore.getEvents()[1]

        assertEquals(shipment1.toString(), uow1.event.toString())
        assertEquals(shipment2.toString(), uow2.event.toString())
        return
    }

    private fun encodePayload(payload: TrackedUnit): ByteBuffer? {
        return UTF_8.encode(Json.encodeToString(payload))
//        val data = ByteBuffer.wrap(b64.toByteArray())
//        val b64 = Base64.getEncoder().encodeToString(payload.toByteArray(Charsets.UTF_8))
    }

    val shipment1 = TrackedUnit().apply {
        id = "SHIP-001"
        timestamp = "2025-01-01T10:00:00Z"
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

    val shipment2 = TrackedUnit().apply {
        id = "SHIP-002"
        timestamp = "2025-01-02T14:30:00Z"
        senderFullName = "Bob Warehouse"
        returnAddress = TrackedUnit.Address(
            street = "500 Warehouse Rd",
            city = "Columbus",
            state = "OH",
            zip = "43004"
        )
        destinationAddress = TrackedUnit.Address(
            street = "45 Elm St",
            city = "Cleveland",
            state = "OH",
            zip = "44101"
        )
        trackingNumber = "TRACK-002-2025"
        weight = 5.75
        dimensions = TrackedUnit.PackageDimensions(
            length = 60.0,
            width = 40.0,
            height = 25.0
        )
    }

    val shipment3 = TrackedUnit().apply {
        id = "SHIP-003"
        timestamp = "2025-01-03T08:15:00Z"
        senderFullName = "Charlie Returns"
        returnAddress = TrackedUnit.Address(
            street = "10 Return Ln",
            city = "Madison",
            state = "WI",
            zip = "53703"
        )
        destinationAddress = TrackedUnit.Address(
            street = "750 Customer Dr",
            city = "Milwaukee",
            state = "WI",
            zip = "53201"
        )
        trackingNumber = "TRACK-003-2025"
        weight = 0.9
        dimensions = TrackedUnit.PackageDimensions(
            length = 20.0,
            width = 15.0,
            height = 5.0
        )
    }

}