package org.myorg.sut

import io.github.huherto.awsLambdaStream.tools.resubmit.ResubmitEvents
import io.kotest.matchers.ints.shouldBeGreaterThan
import io.kotest.matchers.nulls.shouldNotBeNull
import io.kotest.matchers.shouldBe
import kotlinx.coroutines.runBlocking
import kotlinx.datetime.TimeZone
import kotlinx.datetime.number
import kotlinx.datetime.toLocalDateTime
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.myorg.sut.ShipmentTrackingDomain.createFaultEvent
import org.myorg.sut.ShipmentTrackingDomain.createPoisonPillEvent
import kotlin.time.Clock
import kotlin.time.ExperimentalTime

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

    private val queueName = "sut-event-fault-monitor-local-notification-verification.fifo"

    suspend fun purgeSqsQueue() {
        awsFacade.purgeSqsQueue(queueName)
    }

    @OptIn(ExperimentalTime::class)
    @Test
    fun `sns topic should deliver directly published message to verification queue`() : Unit = runBlocking {
        purgeSqsQueue()

        val verificationId = "sns-to-sqs-it-${System.currentTimeMillis()}"
        //val message = """{"verification":"$verificationId"}"""

        val payload = "x".repeat(12_000)
        val message = """{"verification":"$verificationId","payload":"$payload"}"""

        val messageId = awsFacade.publishToSnsTopic(
            topicNameContains = "sut-event-fault-monitor-local.fifo",
            message = message,
            subject = "verification",
            messageGroupId = "verification",
            messageDeduplicationId = verificationId,
        )

        messageId.shouldNotBeNull()

        val notification = awsFacade.verifyNotificationSentToSns(
            queueName = "sut-event-fault-monitor-local-notification-verification.fifo",
            expectedContent = verificationId,
        )

        notification.shouldNotBeNull()
        notification shouldBe message
    }

    @OptIn(ExperimentalTime::class)
    @Test
    fun sendFaultEvent() : Unit = runBlocking {
        purgeSqsQueue()

        val event = createFaultEvent()

        awsFacade.putEvents(event)

        val objectContent = awsFacade.verifyFaultEventStoredInS3(event.id!!)
        objectContent.shouldNotBeNull()
        logger.info { "Fault event found in S3" }
        val notification = awsFacade.verifyNotificationSentToSns(
            queueName = "sut-event-fault-monitor-local-notification-verification.fifo",
            expectedContent = event.id!!,
        )
        notification.shouldNotBeNull()

    }

    @OptIn(ExperimentalTime::class)
    @Test
    fun sendPoisonEvent() : Unit = runBlocking {
        purgeSqsQueue()

        val trackedUnit = ShipmentTrackingDomain.createTrackedUnit()
        val event = createPoisonPillEvent(trackedUnit)
        event.id.shouldNotBeNull()
        logger.info("Poison event is: ${event.id}")
        awsFacade.putEvents(event)

        val objectContent = awsFacade.verifyFaultEventStoredInS3(event.id!!)
        objectContent.shouldNotBeNull()
        logger.info { "Poison event found in S3" }
        val notification = awsFacade.verifyNotificationSentToSns(
            queueName = queueName,
            expectedContent = event.id!!,
        )
        notification.shouldNotBeNull()

        val now = Clock.System.now().toLocalDateTime(TimeZone.UTC)
        val datePart = "%04d/%02d/%02d/%02d".format(now.year, now.month.number, now.day, now.hour)

        val resubmit = ResubmitEvents()
        val argv = ResubmitEvents.Args(
            prefix = "us-east-1/${datePart}/sut-event-fault-monitor-local",
            bucket = "myorg-sut-event-fault-monitor-local-us-east-1",
            functionname = "*",
            dry = true,
            parallel = 16,
            batch = 25,
            async = false,
            batchTimeout = 5_000,
            rate= 3,
            window = 500,
        )

        val preparedEventRequests = resubmit.filterAndPrepareRequests(argv, awsFacade.s3Client)
        preparedEventRequests.size shouldBeGreaterThan 0

        //resubmit.invokeLambdas(argv, preparedEventRequests, awsFacade.lambdaClient)

    }



}