package org.myorg.sut

import io.kotest.matchers.nulls.shouldNotBeNull
import io.kotest.matchers.shouldBe
import kotlinx.coroutines.runBlocking
import mu.KotlinLogging
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.myorg.sut.ShipmentTrackingDomain.createShipmentCreatedEvent
import org.myorg.sut.ShipmentTrackingDomain.createTrackedUnit
import org.myorg.sut.facades.AwsFacade
import org.myorg.sut.facades.S3Facade

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class EventLakeS3ITest {

    private val logger = KotlinLogging.logger {}

    private val awsFacade = AwsFacade()

    private val s3Facade = S3Facade()

    private val bucketName = "myorg-sut-event-lake-s3-local-us-east-1"

    @Test
    fun `message sent to event bus should be saved in event lake S3 bucket`(): Unit = runBlocking {
        val trackedUnit = createTrackedUnit()
        val event = createShipmentCreatedEvent(trackedUnit)

        event.id.shouldNotBeNull()
        logger.info { "Sending event to event bus: ${event.id}" }

        awsFacade.putEvents(event)

        val objectContent = s3Facade.findObjectWithSubstring(
            bucketName = bucketName,
            substring = event.id!!,
        )

        objectContent.shouldNotBeNull()
        objectContent.contains(event.id!!) shouldBe true
        objectContent.contains(event.eventType()) shouldBe true

        logger.info { "Event found in event lake S3 bucket: ${event.id}" }
    }

    @AfterAll
    fun tearDownAll() {
        awsFacade.closeAll()
        s3Facade.close()
    }
}