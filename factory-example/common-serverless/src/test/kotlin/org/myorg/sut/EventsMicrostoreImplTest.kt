package org.myorg.sut

import aws.sdk.kotlin.services.dynamodb.DynamoDbClient
import aws.sdk.kotlin.services.dynamodb.model.PutItemRequest
import aws.sdk.kotlin.services.dynamodb.model.PutItemResponse
import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.Json
import org.junit.jupiter.api.Assertions.*

import java.time.Clock
import java.time.Instant
import java.util.stream.Stream
import kotlin.test.Test

class EventsMicrostoreImplTest {

    @Test
    fun testSave() {

        // Set up
        val ddbClient = mockk<DynamoDbClient>()
        val clock = mockk<Clock>()
        val envConfig = mockk<EnvironmentConfig>()
        val microstore = EventsMicrostoreImpl<MyEvent>(ddbClient, clock, envConfig)
        val putRequestSlot = slot<PutItemRequest>()

        //  Given
        every { runBlocking {  ddbClient.putItem(capture(putRequestSlot)) } } returns mockk<PutItemResponse>()
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
        val json = Json { ignoreUnknownKeys = true }
        putRequestSlot.captured.item?.apply {

            assertEquals("my-event-id-001", this["pk"]?.asS())
            assertEquals("EVENT", this["sk"]?.asS().toString())
            assertEquals("EVENT", this["discriminator"]?.asS().toString())
            assertEquals("1640995200", this["timestamp"]?.asN())
            assertEquals("us-east-1", this["awsregion"]?.asS().toString())
            assertEquals("1743465600", this["ttl"]?.asN())
            assertEquals("1743465600", this["expire"]?.asN())
            assertEquals("null", this["data"]?.asSOrNull().toString())

            val actualEventAsJson = this["event"]?.asS()

            val expectedEventAsJson = """
                {
                    "id":"my-event-id-001",
                    "timestamp":1640995200,
                    "entity": {
                        "id":"my-thing-id-01"
                    }
                }
            """

            assertEquals(
                cleanUpString(expectedEventAsJson),
                cleanUpString(actualEventAsJson.toString()))
        }
        putRequestSlot.captured.tableName.apply {
            assertEquals("events", this)
        }

    }

    fun cleanUpString(s : String) : String {
        return s.replace("\\s".toRegex(), "")
    }
}