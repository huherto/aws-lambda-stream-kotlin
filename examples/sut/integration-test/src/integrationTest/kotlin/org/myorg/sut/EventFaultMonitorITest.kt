package org.myorg.sut

import io.kotest.matchers.nulls.shouldNotBeNull
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.myorg.sut.ShipmentTrackingDomain.createFaultEvent

// Components tested.
//
//   - A fault event is created and sent to event bridge.
//   - Event bridge will send the event to the firehose delivery stream.
//   - Firehose delivery stream will send the fault event to S3. myorg-sut-event-fault-monitor-local-us-east-1
//   - We should verify that the fault event is stored in S3.
//   - The Transform function should send a notification to SNS.
//   - We should verify that the notification is sent to SNS.

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class EventFaultMonitorITest {

    private val logger = mu.KotlinLogging.logger {}

    private val awsFacade = AwsFacade(eventTable = "sut-control-service-local-events")

    @Test
    fun sendFaultEvent() : Unit = runBlocking {

        val event = createFaultEvent()

        awsFacade.putEvents(event)

        val objectContent = awsFacade.verifyFaultEventStoredInS3(event.id!!)
        objectContent.shouldNotBeNull()
        logger.info { "Fault event stored in S3: $objectContent" }
        val notification = awsFacade.verifyNotificationSentToSns(
            queueName = "sut-event-fault-monitor-local-notification-verification.fifo",
            expectedContent = event.id!!,
        )
        notification.shouldNotBeNull()
        logger.info { "SNS notification received: $notification" }

    }

}