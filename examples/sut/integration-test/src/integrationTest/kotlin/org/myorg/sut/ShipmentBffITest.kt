package org.myorg.sut

import io.kotest.matchers.nulls.shouldNotBeNull
import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNotBe
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.AfterAll
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

    private val awsFacade = AwsFacade()

    @OptIn(ExperimentalTime::class)
    @Test
    fun happyPath() : Unit = runBlocking {
        val event = createShipmentCreatedEvent(ShipmentTrackingDomain.createTrackedUnit())
        event.id.shouldNotBeNull()
        event.entity.shouldNotBeNull()
        val entityId = event.entity?.id.shouldNotBeNull()

        logger.info { "Sending event ${event.id}" }
        awsFacade.putEvents(event)

        val savedEntity = awsFacade.findEntityByPK(entityId) { it?.firstOrNull() }
        savedEntity.shouldNotBeNull()
        with(savedEntity) {
            this["pk"]?.asS() shouldBe entityId
            this["sk"]?.asS() shouldBe("SHIPMENT")
            this["senderFullName"]?.asS() shouldBe "John Doe"
            this["trackingNumber"]?.asS() shouldBe "TRK123456789"
            this["weight"]?.asN() shouldBe "10.5"
            this["dimensions.height"]?.asN() shouldBe "6.0"
            this["dimensions.width"]?.asN() shouldBe "8.0"
            this["dimensions.length"]?.asN() shouldBe "12.0"
            this["destinationAddress"]?.asS() shouldNotBe null
            this["returnAddress"]?.asS() shouldNotBe null

        }


    }

    @AfterAll
    fun tearDownAll() {
        awsFacade.closeAll()
    }
}