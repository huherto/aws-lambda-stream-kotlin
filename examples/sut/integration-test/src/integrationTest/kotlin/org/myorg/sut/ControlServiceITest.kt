package org.myorg.sut

import aws.sdk.kotlin.services.dynamodb.model.AttributeValue
import aws.sdk.kotlin.services.eventbridge.model.PutEventsRequest
import aws.sdk.kotlin.services.eventbridge.model.PutEventsRequestEntry
import aws.smithy.kotlin.runtime.net.url.Url
import io.github.huherto.awsLambdaStream.JsonEvent
import io.kotest.matchers.longs.shouldBeGreaterThan
import io.kotest.matchers.longs.shouldBeLessThan
import io.kotest.matchers.nulls.shouldNotBeNull
import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNotBe
import io.kotest.matchers.string.shouldMatch
import io.kotest.matchers.string.shouldNotBeBlank
import io.kotest.matchers.string.shouldNotBeEmpty
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.lang.System.currentTimeMillis
import kotlin.math.abs
import kotlin.random.Random
import kotlin.time.ExperimentalTime

typealias DBRecord = Map<String, AttributeValue?>

// Components tested.
//   - Send event to event bridge. sut-event-hub-local-bus.
//   - Event bridge will send event to kinesis stream. sut-event-hub-local-s1
//   - sut-control-service-local-listener will read event from the kinesis stream.
//   - sut-control-service-local-listener will insert the event in DynamoDB. sut-control-service-local-events
//   - sut-control-service-local-trigger will read events from DynamoDB Stream
//   - sut-control-service-local-trigger will insert correlation events in DynamoDB
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ControlServiceITest {

    private val logger = mu.KotlinLogging.logger {}

    fun endPointUrl() = Url.parse("http://localhost:4566")

    @OptIn(ExperimentalTime::class)
    @Test
    fun sendEvents() : Unit = runBlocking {

        val event = createShipmentCreatedEvent(createTrackedUnit())
        event.id.shouldNotBeNull()
        event.entity.shouldNotBeNull()
        event.entity?.id.shouldNotBeNull()

        logger.info { "Sending event ${event.id}" }
        val res = AwsFacade.eventBridgeClient.putEvents(PutEventsRequest {
            entries = listOf(
                PutEventsRequestEntry {
                    eventBusName = "sut-event-hub-local-bus"
                    detail = event.encoded()
                    detailType = "my-event"
                    source = "integration-test"
                }
            )
        })
        res.failedEntryCount shouldBe 0

        // Find collected event in DynamoDB.
        val collectedEvent = AwsFacade.findEventByPK(event.id!!) { items ->
            items?.firstOrNull() ?: return@findEventByPK null
        }
        with(collectedEvent) {
            this.shouldNotBeNull()
            this["pk"]?.asS() shouldBe event.id
            this["sk"]?.asS() shouldBe "EVENT"
            this["discriminator"]?.asS() shouldBe "EVENT"
            this["data"]?.asS() shouldBe event.entity?.id
            this["event"]?.asS().shouldNotBeNull()

            val timeStamp = this["timestamp"]?.asN()?.toLong()
            checkTimestampDiff(timeStamp, event.timestamp)
        }

        // Find correlated event in DynamoDB.
        val correlEvent = AwsFacade.findEventByPK(event.entity?.id!!) { items ->
            items?.firstOrNull { rec -> rec["sk"]?.asS() == event.id }
        }
        with(correlEvent) {
            this.shouldNotBeNull()
            checkDbRecord(
                dbrecord = this,
                pk = event.entity?.id!!,
                sk = event.id,
                discriminator = "CORREL",
                pipelineId = "corre1")
            val eventAsObject = getEventAsObject(this)
            eventAsObject.eventType().shouldBe("SHIPMENT_CREATED")
            eventAsObject.partitionKey shouldBe event.entity?.id
        }

        // Finding VERIFY_TARGET_ADDRESS among the correlated events.
        val vtaCorrelEvent = AwsFacade.findEventByPK(event.entity?.id!!) { items ->
            items?.firstOrNull { rec -> rec["event"]?.asS()?.contains("VERIFY_TARGET_ADDRESS") == true }
        }
        vtaCorrelEvent.shouldNotBeNull()
        val vtaEventId = with(vtaCorrelEvent) {
            logger.debug { "vtaEvent: $vtaCorrelEvent" }
            this.shouldNotBeNull()
            checkDbRecord(
                dbrecord = this,
                pk = event.entity?.id!!,
                sk = null,
                discriminator = "CORREL",
                pipelineId = "corre1")
            val vtaEventAsObject = getEventAsObject(this)
            vtaEventAsObject.shouldNotBeNull()
            val vtaEventId = vtaEventAsObject.id
            vtaEventId.shouldNotBeNull()
            vtaEventId.endsWith(".eval_vta") shouldBe true
            vtaEventAsObject.eventType().shouldBe("VERIFY_TARGET_ADDRESS")
            vtaEventAsObject.partitionKey shouldBe event.entity?.id

            vtaEventId
        }

        // Finding VERIFY_TARGET_ADDRESS by its event id.
        val vtaEvent = AwsFacade.findEventByPK(vtaEventId) { items ->
            items?.firstOrNull()
        }
        with(vtaEvent) {
            logger.debug { "vtaEvent: $this" }
            this.shouldNotBeNull()
            checkDbRecord(
                dbrecord = this,
                pk = vtaEventId,
                sk = "EVENT",
                discriminator = "EVENT",
                pipelineId = "collect1")
            val vtaEventAsObject = JsonEvent(this["event"]?.asS() ?: "{}")
            vtaEventAsObject.shouldNotBeNull()
            val vtaEventId = vtaEventAsObject.id
            vtaEventId.shouldNotBeNull()
            vtaEventId.endsWith(".eval_vta") shouldBe true
            vtaEventAsObject.eventType().shouldBe("VERIFY_TARGET_ADDRESS")
            vtaEventAsObject.partitionKey shouldBe event.entity?.id

            vtaEventId
        }

        // Make two failed attempts to deliver the package.
        val e1 = createDeliveryAttemptedEvent(event.entity!!)
        val e2 = createDeliveryAttemptedEvent(event.entity!!)
        run {
            val res = AwsFacade.eventBridgeClient.putEvents(PutEventsRequest {
                entries = listOf(
                    PutEventsRequestEntry {
                        eventBusName = "sut-event-hub-local-bus"
                        detail = e1.encoded()
                        detailType = "my-event"
                        source = "integration-test"
                    },
                    PutEventsRequestEntry {
                        eventBusName = "sut-event-hub-local-bus"
                        detail = e2.encoded()
                        detailType = "my-event"
                        source = "integration-test"
                    }
                )
            })
            res.failedEntryCount shouldBe 0
        }
        val ccEvent = AwsFacade.findEventByPK(event.entity?.id!!) { items ->
            items?.firstOrNull { rec -> rec["event"]?.asS()?.contains("CONTACT_CUSTOMER") == true }
        }
        with(ccEvent) {
            this.shouldNotBeNull()
            checkDbRecord(
                dbrecord = this,
                pk = event.entity?.id!!,
                sk = null,
                discriminator = "CORREL",
                pipelineId = "corre1")
            val ccEventAsObject = getEventAsObject(this)
            ccEventAsObject.shouldNotBeNull()
            ccEventAsObject.eventType().shouldBe("CONTACT_CUSTOMER")
        }

        val kinesisEvents = AwsFacade.readAllKinesisEvents()
        kinesisEvents shouldNotBe null
        logger.info { "Read ${kinesisEvents.size} events from Kinesis" }
        println("Read ${kinesisEvents.size} events from Kinesis")
        for (kinesisEvent in kinesisEvents) {
            logger.debug { "Kinesis event: $kinesisEvent" }
        }
    }

    private fun getEventAsObject(map: DBRecord) : JsonEvent {
        val eventAsString = map.get("event")?.asSOrNull()
        eventAsString.shouldNotBeNull()
        val eventAsObject = JsonEvent(eventAsString)
        eventAsObject.shouldNotBeNull()
        eventAsObject.id.shouldNotBeNull()
        eventAsObject.id.shouldNotBeBlank()
        eventAsObject.partitionKey.shouldNotBeNull()
        eventAsObject.partitionKey.shouldNotBeBlank()
        eventAsObject.eventType().shouldNotBeNull()
        eventAsObject.eventType().shouldNotBeBlank()
        eventAsObject.timestamp.shouldNotBeNull()

        return eventAsObject
    }

    private fun checkDbRecord(
        dbrecord: DBRecord?,
        pk: String,
        sk: String?,
        discriminator: String,
        pipelineId: String?,
    ) {
        dbrecord.shouldNotBeNull()
        dbrecord["pk"]?.asS() shouldBe pk
        dbrecord["sk"]?.asSOrNull().shouldNotBeNull()
        dbrecord["sk"]?.asSOrNull().shouldNotBeEmpty()
        sk?.let { dbrecord["sk"]?.asS() shouldBe sk }
        dbrecord["discriminator"]?.asS() shouldBe discriminator
        dbrecord["expire"]?.asBoolOrNull() shouldBe false
        dbrecord["awsregion"]?.asS() shouldBe "us-east-1"
        dbrecord["suffix"]?.asS() shouldBe ""
        pipelineId?.let { dbrecord["pipelineId"]?.asS() shouldBe pipelineId }

        val sequenceNumber = dbrecord["sequenceNumber"]?.asS()
        sequenceNumber.shouldNotBeNull()
        sequenceNumber shouldMatch "\\d+".toRegex()

        dbrecord["event"]?.asS().shouldNotBeNull()

        val ttl = dbrecord["ttl"]?.asN()?.toLong()
        ttl.shouldNotBeNull()
        ttl shouldBeGreaterThan 1742326911L // A date in 2025
        ttl shouldBeLessThan 1900093311L // A date in 2030

        val timeStamp = dbrecord["timestamp"]?.asN()?.toLong()
        checkTimestampDiff(currentTimeMillis(), timeStamp)
    }

    private fun checkTimestampDiff(t1: Long?, t2: Long?) {
        t1.shouldNotBeNull()
        t2.shouldNotBeNull()
        val diff = abs(t1 - t2)
        diff shouldBeLessThan 100 * 1000L
    }

    @Test
    fun sendPoisonPillEvent() : Unit = runBlocking {

        val event = createPoisonPillEvent(createTrackedUnit())

        val res = AwsFacade.eventBridgeClient.putEvents(PutEventsRequest {
            entries = listOf(
                PutEventsRequestEntry {
                    eventBusName = "sut-event-hub-local-bus"
                    detail = event.encoded()
                    detailType = "my-event"
                    source = "integration-test"
                }
            )
        })
        res.failedEntryCount shouldBe 0
    }

    @AfterAll
    fun tearDownAll() {
        AwsFacade.closeAll()
    }

    private fun createTrackedUnit() = TrackedUnit().apply {
    id = "unit-" + generateRandomNumber()
        senderFullName = "John Doe"
        returnAddress = TrackedUnit.Address("123 Main St", "Atlanta", "GA", "30303")
        destinationAddress = TrackedUnit.Address("456 Oak St", "Miami", "FL", "33101")
        trackingNumber = "TRK123456789"
        weight = 10.5
        dimensions = TrackedUnit.PackageDimensions(12.0, 8.0, 6.0)
    }

    private fun generateRandomNumber(): String {
        val rand = Random.Default
        return String.format("%04d", rand.nextLong(10000))
    }

    private fun createShipmentCreatedEvent(trackedUnit: TrackedUnit) = ShipmentCreatedEvent().apply {
        id = "ship-" + generateRandomNumber()
        partitionKey = trackedUnit.id
        timestamp = System.currentTimeMillis()
        location = "Atlanta Hub"
        entity = trackedUnit
    }

    private fun createDeliveryAttemptedEvent(trackedUnit: TrackedUnit) = DeliveryAttemptedEvent().apply {
        id = "ship-" + generateRandomNumber()
        partitionKey = trackedUnit.id
        timestamp = System.currentTimeMillis()
        location = "Atlanta Hub"
        reason = "Can't access the door"
        entity = trackedUnit
    }

    private fun createPoisonPillEvent(trackedUnit: TrackedUnit) = ShipmentCreatedEvent().apply {
        id = "poison-" + generateRandomNumber()
        partitionKey = trackedUnit.id
        timestamp = System.currentTimeMillis()
        location = "poison-pill"
        entity = trackedUnit
    }
}