
package org.myorg.sut

import io.kotest.matchers.shouldBe
import org.junit.jupiter.api.Test
import java.util.*

class TracerEventCodecTest {

    @Test
    fun `should roundtrip TracerEvent`() {
        val tracer = Tracer(
            awsRegion = "us-east-1",
            roundedTimestamp = 1234567890L,
            timestamp = 1234567891L,
            ttl = 999999999L,
            status = "STARTED"
        )
        val event = TracerEvent(
            tracer = tracer,
            id = UUID.randomUUID().toString(),
            partitionKey = "us-east-1"
        )

        val encoded = TracerEventCodec.encode(event)
        println("Encoded: $encoded")
        
        val decoded = TracerEventCodec.decode(encoded) as TracerEvent
        
        decoded.tracer shouldBe tracer
        decoded.id shouldBe event.id
        decoded.partitionKey shouldBe event.partitionKey
    }
}
