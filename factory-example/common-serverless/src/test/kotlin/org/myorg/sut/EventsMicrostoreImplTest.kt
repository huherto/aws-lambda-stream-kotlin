package org.myorg.sut

import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import org.junit.jupiter.api.Assertions.*
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest
import software.amazon.awssdk.services.dynamodb.model.PutItemResponse
import java.time.Clock
import java.time.Instant
import java.util.stream.Stream
import kotlin.test.Test

class EventsMicrostoreImplTest {

    @Test
    fun testSave() {

        val ddbClient = mockk<DynamoDbClient>()
        val clock = mockk<Clock>()
        val envConfig = mockk<EnvironmentConfig>()
        val microstore = EventsMicrostoreImpl<TrackedUnit>(ddbClient, clock, envConfig)
        val putRequestSlot = slot<PutItemRequest>()

        every { ddbClient.putItem(capture(putRequestSlot)) } returns mockk<PutItemResponse>()
        every { clock.instant() } returns Instant.parse("2025-01-01T00:00:00.000Z")
        every { envConfig.awsRegion() } returns "us-east-1"
        every { envConfig.tableName() } returns "events"

        val stream = Stream.of(
            UnitOfWork<TrackedUnit>().apply {
                event = TrackedUnit().apply {
                    id = "id1"
                    timestamp = "2022-01-01T00:00:00.000Z"
                }
            },
        )
        microstore.save(stream, EventsMicrostore.SaveOptions(expire = 90))

        putRequestSlot.captured.item().apply {
            assertEquals("id1", this["pk"]?.s())
            assertEquals("EVENT", this["sk"]?.s().toString())
            assertEquals("EVENT", this["discriminator"]?.s().toString())
            assertEquals("2022-01-01T00:00:00.000Z", this["timestamp"]?.s().toString())
            assertEquals("us-east-1", this["awsregion"]?.s().toString())
            assertEquals("1743465600", this["ttl"]?.n().toString())
            assertEquals("1743465600", this["expire"]?.n().toString())
            assertEquals("null", this["data"]?.s().toString())
            // assertEquals("{\"id\":\"id1\",\"timestamp\":\"2022-01-01T00:00:00.000Z\"}", this["event"]?.s().toString())
        }
        putRequestSlot.captured.tableName().apply {
            assertEquals("events", this)
        }

    }

}