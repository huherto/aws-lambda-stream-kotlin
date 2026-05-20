package org.myorg.sut

import io.github.huherto.awsLambdaStream.JsonEvent
import io.kotest.matchers.equality.shouldBeEqualToComparingFields
import io.kotest.matchers.nulls.shouldNotBeNull
import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNotBe
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

// Components tested.
//   - Send event to event bridge. sut-event-hub-local-bus.
//   - Event bridge will send event to kinesis stream. sut-event-hub-local-s1
//   - sut-shipment-bff-local-listener will read event from the kinesis stream.
//   - sut-shipment-bff-local-listener will insert the entity in DynamoDB. sut-shipment-bff-local-entity
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ShipmentBffITest {
    private val logger = mu.KotlinLogging.logger {}

    private val awsFacade = AwsFacade(
        entityTable = "sut-shipment-bff-local-shipments",
        eventTable = "sut-control-service-local-events"
    )

    private val restApiFacade = RestApiFacade()

    @Test
    fun postShipmentToRestApi() : Unit = runBlocking {
        val shipment = ShipmentTrackingDomain.createTrackedUnit()
        val shipmentId = shipment.id.shouldNotBeNull()

        postShipment(shipment)

        verifyShipmentWasSaved(shipmentId)

        verifyGetShipmentReturned(shipmentId, shipment)

        verifyShipmentCreatedEventWasPublished(shipmentId)

    }

    private suspend fun verifyShipmentCreatedEventWasPublished(shipmentId: String) {
        val shipmentCreatedEvent = awsFacade.findEventByPK(shipmentId) { events ->
            events?.firstOrNull {
                val eventAsString = it.get("event")?.asSOrNull()
                logger.info { "Event: $eventAsString" }
                eventAsString.shouldNotBeNull()
                val eventAsObject = JsonEvent(eventAsString)
                eventAsObject.eventType() == "SHIPMENT_CREATED"
            }
        }
        shipmentCreatedEvent.shouldNotBeNull()
    }

    private fun verifyGetShipmentReturned(shipmentId: String, shipment: TrackedUnit) {
        val returnedShipment = restApiFacade.get(shipmentId)
        returnedShipment.shouldNotBeNull()
        returnedShipment.shouldBeEqualToComparingFields(shipment)
    }

    private suspend fun postShipment(shipment: TrackedUnit) {
        val response = restApiFacade.post(shipment)

        response.statusCode shouldBe 201
        response.headers["Content-Type"] shouldBe "application/json"
        response.body.shouldNotBeNull()
    }

    private suspend fun verifyShipmentWasSaved(shipmentId: String) {
        val savedEntity = awsFacade.findEntityByPK(shipmentId) { it?.firstOrNull() }
        savedEntity.shouldNotBeNull()
        with(savedEntity) {
            this["pk"]?.asS() shouldBe shipmentId
            this["sk"]?.asS() shouldBe "SHIPMENT"
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