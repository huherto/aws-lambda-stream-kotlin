package org.myorg.sut

import io.github.huherto.awsLambdaStream.tools.ReplayEvents
import io.kotest.matchers.ints.shouldBeGreaterThan
import io.kotest.matchers.ints.shouldBeLessThanOrEqual
import io.kotest.matchers.maps.shouldContainKey
import io.kotest.matchers.nulls.shouldNotBeNull
import io.kotest.matchers.shouldBe
import kotlinx.coroutines.runBlocking
import kotlinx.datetime.TimeZone
import kotlinx.datetime.number
import kotlinx.datetime.toLocalDateTime
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.myorg.sut.ShipmentTrackingDomain.createShipmentCreatedEvent
import org.myorg.sut.ShipmentTrackingDomain.createTrackedUnit
import org.myorg.sut.facades.EventBridgeFacade
import org.myorg.sut.facades.LambdaFacade
import org.myorg.sut.facades.S3Facade
import kotlin.time.Clock
import kotlin.time.ExperimentalTime

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ReplayEventsITest {

    private val logger = mu.KotlinLogging.logger {}

    private val eventBridgeFacade = EventBridgeFacade()
    private val lambdaFacade = LambdaFacade()
    private val s3Facade = S3Facade()

    private val bucketName = "myorg-sut-event-lake-s3-local-us-east-1"

    @OptIn(ExperimentalTime::class)
    @Test
    fun `event saved in event lake can be replayed to listener lambda`(): Unit = runBlocking {
        val event = createShipmentCreatedEvent(createTrackedUnit())

        event.id.shouldNotBeNull()
        logger.info { "Sending event to event bus before replay: ${event.id}" }

        eventBridgeFacade.putEvents(event)

        val objectContent = s3Facade.findObjectWithSubstring(
            bucketName = bucketName,
            substring = event.id!!,
        )

        objectContent.shouldNotBeNull()
        objectContent.contains(event.id!!) shouldBe true
        objectContent.contains(event.eventType()) shouldBe true

        val now = Clock.System.now().toLocalDateTime(TimeZone.UTC)
        val prefix = "us-east-1/%04d/%02d/%02d/".format(
            now.year,
            now.month.number,
            now.day,
        )

        val replayEvents = ReplayEvents()
        val counters = replayEvents.runReplayEvents(
            argv = ReplayEvents.Args(
                bucket = bucketName,
                prefix = prefix,
                type = event.eventType(),
                functionname = "sut-control-service-local-listener",
                dry = false,
                async = false,
                batchTimeout = 1_000,
                parallel = 1,
                rate = 10,
                window = 100,
            ),
            s3 = s3Facade.client,
            lambda = lambdaFacade.client,
        )

        counters.errors shouldBe 0
        counters.match shouldBeGreaterThan 0
        counters.recordCount shouldBeGreaterThan 0
        counters.types shouldContainKey event.eventType()
        counters.invoked.shouldNotBeNull()
        val total = counters.invoked?.total.shouldNotBeNull()
        total shouldBeGreaterThan 0
        total shouldBeLessThanOrEqual counters.recordCount
        counters.invoked?.statuses?.shouldContainKey(200)
    }


    @AfterAll
    fun tearDownAll() {
        eventBridgeFacade.close()
        lambdaFacade.close()
        s3Facade.close()
    }
}