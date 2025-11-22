package org.myorg.sut

import com.amazonaws.services.lambda.runtime.events.KinesisEvent
import com.amazonaws.services.lambda.runtime.tests.annotations.Event
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.params.ParameterizedTest
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class KinesisHandlerTest {

    private var eventsMicrostore = EventsMicrostoreFake<TrackedUnit>()

    private var kinesisAdapter : KinesisAdapter<TrackedUnit> = KinesisAdapter<TrackedUnit>()

    private var kinesisHandler : KinesisHandler? = KinesisHandler(eventsMicrostore, kinesisAdapter)

    class MyKinesisAdapter : KinesisAdapter<TrackedUnit>() {
        override inline fun <reified T> decodePayload(data: ByteBuffer): T {
            return parseJson<T>(utf8Decode(data))
        }
    }

    @BeforeEach
    fun beforeEach() {
        eventsMicrostore.reset();
    }

    @ParameterizedTest
    @Event(value = "events/kinesis_basic.json", type = KinesisEvent::class)
    fun testBasicKinesisEvent(event: KinesisEvent): Unit {
        val context = TestContext()
        val payload = """{"orderId":123,"status":"NEW"}"""
        event.records.get(0).kinesis.setData(encodePayload(payload))
        kinesisHandler!!.handleRequest(event, context)
        assertEquals(1,eventsMicrostore.getEvents().size)
        val processedEvent = eventsMicrostore.getEvents().first()
        val data = processedEvent.records.first().kinesis.data.asReadOnlyBuffer().rewind()
        val decoded = StandardCharsets.UTF_8.decode(data).toString()
        assertTrue(decoded.contains("123"))
        return
    }

    private fun encodePayload(payload: String): ByteBuffer? {
//        val b64 = Base64.getEncoder().encodeToString(payload.toByteArray(Charsets.UTF_8))
//        val data = ByteBuffer.wrap(b64.toByteArray())
    return  StandardCharsets.UTF_8.encode(payload)
    }
}