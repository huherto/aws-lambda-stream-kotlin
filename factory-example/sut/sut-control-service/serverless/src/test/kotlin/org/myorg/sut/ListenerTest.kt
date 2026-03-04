package org.myorg.sut

import aws.sdk.kotlin.services.dynamodb.DynamoDbClient
import aws.sdk.kotlin.services.dynamodb.model.PutItemRequest
import aws.sdk.kotlin.services.dynamodb.model.PutRequest
import com.amazonaws.services.lambda.runtime.events.KinesisEvent
import io.github.huherto.awsLambdaStream.EnvironmentConfig
import io.github.huherto.awsLambdaStream.testsupport.DynamDbClientFake
import io.github.huherto.awsLambdaStream.testsupport.TestContext
import io.mockk.every
import io.mockk.mockk
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.params.ParameterizedTest
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets.UTF_8
import java.time.Instant
import kotlin.test.assertEquals
import kotlin.test.assertNotNull

typealias TestEvent = com.amazonaws.services.lambda.runtime.tests.annotations.Event

class ListenerTest {

    private var kinesisAdapter = MyKinesisAdapter()

    private val dynamoDbClient = DynamDbClientFake(mockk<DynamoDbClient>())

    private val listener : Listener by lazy {
        val envConfig = mockk<EnvironmentConfig>()
        every { envConfig.awsRegion() } returns "us-east-1"
        every { envConfig.tableName() } returns "events"
        every { envConfig.ttl() } returns null
        Listener(null, kinesisAdapter, envConfig, dynamoDbClient)
    } 

    @BeforeEach
    fun beforeEach() {
        dynamoDbClient.reset()
    }

    @ParameterizedTest
    @TestEvent(value = "events/kinesis_basic.json", type = KinesisEvent::class)
    fun testBasicKinesisEvent(event: KinesisEvent) = runBlocking {
        val context = TestContext()

        event.records[0].kinesis.data = encodePayload(ship1Event1)
        event.records[1].kinesis.data = encodePayload(ship1Event2)

        listener.handleRequest(event, context)

        assertEquals(2,dynamoDbClient.putRequests.size)
        val putRequest1 = dynamoDbClient.putRequests["SHIP-001-2025"]
        val putRequest2 = dynamoDbClient.putRequests["SHIP-002-2025"]

        assertNotNull(putRequest1)
        assertNotNull(putRequest2)
        assertJsonString(ship1Event1.toString(), putRequest1.item?.get("event")!!.asS())
        assertJsonString(ship1Event2.toString(), putRequest2.item?.get("event")!!.asS())
    }

    private fun cleanUpString(s: String): String {
        return s.replace("\\s".toRegex(), "")
    }

    private fun assertJsonString(expected: String, actual: String) {
        assertEquals(cleanUpString(expected), cleanUpString(actual))
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