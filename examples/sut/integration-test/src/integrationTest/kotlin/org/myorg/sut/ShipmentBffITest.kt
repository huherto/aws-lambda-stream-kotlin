package org.myorg.sut

import io.kotest.matchers.nulls.shouldNotBeNull
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.myorg.sut.ShipmentTrackingDomain.createShipmentCreatedEvent
import kotlin.time.ExperimentalTime

// Components tested.
//   - Send event to event bridge. sut-event-hub-local-bus.
//   - Event bridge will send event to kinesis stream. sut-event-hub-local-s1
//   - sut-shipment-bff-local-listener will read event from the kinesis stream.
//   - sut-shipment-bff-local-listener will insert the entity in DynamoDB. sut-shipment-bff-local-entity
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ShipmentBffITest {
    private val logger = mu.KotlinLogging.logger {}

    @OptIn(ExperimentalTime::class)
    @Test
    fun happyPath() : Unit = runBlocking {
        val event = createShipmentCreatedEvent(ShipmentTrackingDomain.createTrackedUnit())
        event.id.shouldNotBeNull()
        event.entity.shouldNotBeNull()
        event.entity?.id.shouldNotBeNull()

        logger.info { "Sending event ${event.id}" }
        AwsFacade.putEvents(event)

    }
}