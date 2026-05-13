package org.myorg.sut

import io.github.huherto.awsLambdaStream.EnvironmentConfig
import io.github.huherto.awsLambdaStream.FaultManager
import io.github.huherto.awsLambdaStream.sinks.EventsMicrostore
import io.kotest.matchers.nulls.shouldNotBeNull
import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.shouldBeInstanceOf
import io.mockk.mockk
import org.junit.jupiter.api.Test
import java.nio.ByteBuffer

class ListenerContainerTest {

    @Test
    fun `MyKinesisAdapter should decode valid JSON payload into TrackedUnitEvent`() {
        // Arrange
        val faultManager: FaultManager = mockk(relaxed = true)
        val adapter = ListenerContainer.MyKinesisAdapter(faultManager)
        val originalEvent = ShipmentCreatedEvent().apply {
            id = "test-event-123"
            partitionKey = "pk-1"
        }
        val jsonString = jsonEncode(originalEvent)
        val payload = ByteBuffer.wrap(jsonString.toByteArray(Charsets.UTF_8))

        // Act
        val result = adapter.decodePayload(payload)

        // Assert
        result.shouldNotBeNull()
        result.shouldBeInstanceOf<ShipmentCreatedEvent>()
        result.id shouldBe "test-event-123"
        result.partitionKey shouldBe "pk-1"
    }

    @Test
    fun `Container properties should be initialized correctly`() {
        // Arrange
        val envConfig: EnvironmentConfig = mockk(relaxed = true)
        val eventsMicrostore: EventsMicrostore = mockk(relaxed = true)
        val faultManager: FaultManager = mockk(relaxed = true)

        val container = ListenerContainer(
            envConfig = envConfig,
            eventsMicrostore = eventsMicrostore,
            faultManager = faultManager
        )

        // Act & Assert
        val kinesisAdapter = container.kinesisAdapter
        kinesisAdapter.shouldNotBeNull()
        kinesisAdapter.shouldBeInstanceOf<ListenerContainer.MyKinesisAdapter>()

        val assembler = container.assembler
        assembler.shouldNotBeNull()
    }
}