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
        val microstore = EventsMicrostoreImpl<MyEvent>(ddbClient, clock, envConfig)
        val putRequestSlot = slot<PutItemRequest>()
        //  Given
        every { ddbClient.putItem(capture(putRequestSlot)) } returns mockk<PutItemResponse>()
        every { clock.instant() } returns Instant.parse("2025-01-01T00:00:00.000Z")
        every { envConfig.awsRegion() } returns "us-east-1"
        every { envConfig.tableName() } returns "events"

        // When
        val stream = Stream.of(
            UnitOfWork<MyEvent>().apply {
                event = MyEvent().apply {
                    id = "my-event-id-001"
                    timestamp = Instant.parse("2022-01-01T00:00:00.000Z").toEpochMilli()/1000
                    entity = MyThing().apply {
                        id = "my-thing-id-01"
                    }
                }
            },
        )
        microstore.save(stream, EventsMicrostore.SaveOptions(expireDays = 90))

        // Then
        putRequestSlot.captured.item().apply {
            assertEquals("my-event-id-001", this["pk"]?.s())
            assertEquals("EVENT", this["sk"]?.s().toString())
            assertEquals("EVENT", this["discriminator"]?.s().toString())
            assertEquals("1640995200", this["timestamp"]?.n())
            assertEquals("us-east-1", this["awsregion"]?.s().toString())
            assertEquals("1743465600", this["ttl"]?.n())
            assertEquals("1743465600", this["expire"]?.n())
            assertEquals("null", this["data"]?.s().toString())
            // assertEquals("{\"id\":\"id1\",\"timestamp\":\"2022-01-01T00:00:00.000Z\"}", this["event"]?.s().toString())
        }
        putRequestSlot.captured.tableName().apply {
            assertEquals("events", this)
        }

    }

}