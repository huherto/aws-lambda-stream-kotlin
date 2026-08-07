package org.myorg.sut

import io.github.huherto.awsLambdaStream.tools.ResubmitFaults
import io.kotest.matchers.ints.shouldBeGreaterThan
import io.kotest.matchers.nulls.shouldNotBeNull
import io.kotest.matchers.shouldBe
import kotlinx.coroutines.runBlocking
import kotlinx.datetime.TimeZone
import kotlinx.datetime.number
import kotlinx.datetime.toLocalDateTime
import kotlinx.serialization.json.*
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.myorg.sut.ShipmentTrackingDomain.createFaultEvent
import org.myorg.sut.ShipmentTrackingDomain.createPoisonPillEvent
import org.myorg.sut.facades.*
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
    private val json = Json {
        ignoreUnknownKeys = true
    }
    private val eventBridgeFacade = EventBridgeFacade()
    private val lambdaFacade = LambdaFacade()
    private val s3Facade = S3Facade()
    private val snsFacade = SnsFacade()
    private val sqsFacade = SqsFacade()

    private val bucketName = "myorg-sut-event-fault-monitor-local-us-east-1"

    private val queueName = "sut-event-fault-monitor-local-notification-verification.fifo"

    suspend fun purgeSqsQueue() {
        sqsFacade.purgeQueue(queueName)
    }

    @OptIn(ExperimentalTime::class)
    @Test
    fun `sns topic should deliver directly published message to verification queue`() : Unit = runBlocking {
        purgeSqsQueue()

        val verificationId = "sns-to-sqs-it-${System.currentTimeMillis()}"
        //val message = """{"verification":"$verificationId"}"""

        val payload = "x".repeat(12_000)
        val message = """{"verification":"$verificationId","payload":"$payload"}"""

        val messageId = snsFacade.publishToTopic(
            topicNameContains = "sut-event-fault-monitor-local.fifo",
            message = message,
            subject = "verification",
            messageGroupId = "verification",
            messageDeduplicationId = verificationId,
        )

        messageId.shouldNotBeNull()

        val notification = sqsFacade.verifyNotificationSentToSns(
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

        eventBridgeFacade.putEvents(event)

        val objectContent = s3Facade.findObjectWithSubstring(bucketName = bucketName , event.id!!)
        objectContent.shouldNotBeNull()

        val storedEvent = parseStoredEventBridgeEvent(objectContent, event.id!!)
        val detail = storedEvent["detail"].shouldNotBeNull().jsonObject

        detail["id"].shouldNotBeNull().jsonPrimitive.content shouldBe event.id
        detail["type"].shouldNotBeNull().jsonPrimitive.content shouldBe "fault"

        logger.info { "Fault event found in S3" }
        val notification = sqsFacade.verifyNotificationSentToSns(
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
        eventBridgeFacade.putEvents(event)

        val objectContent = s3Facade.findObjectWithSubstring(bucketName = bucketName, event.id!!)
        objectContent.shouldNotBeNull()

        val storedEvent = parseStoredEventBridgeEvent(objectContent, event.id!!)
        assertStoredFaultEventIsResubmittable(storedEvent)
        logger.info { "Poison event found in S3" }
        val notification = sqsFacade.verifyNotificationSentToSns(
            queueName = queueName,
            expectedContent = event.id!!,
        )
        notification.shouldNotBeNull()

        val now = Clock.System.now().toLocalDateTime(TimeZone.UTC)
        val datePart = "%04d/%02d/%02d/%02d".format(now.year, now.month.number, now.day, now.hour)

        val resubmit = ResubmitFaults()
        val argv = ResubmitFaults.Args(
            prefix = "us-east-1/${datePart}/sut-event-fault-monitor-local",
            bucket = "myorg-sut-event-fault-monitor-local-us-east-1",
            functionname = "sut-control-service-local-listener",
            dry = true,
            parallel = 16,
            batch = 25,
            async = false,
            batchTimeout = 5_000,
            rate= 3,
            window = 500,
        )

        val preparedEventRequests = resubmit.filterAndPrepareRequests(argv, s3Facade.client)
        preparedEventRequests.size shouldBeGreaterThan 0

        resubmit.invokeLambdas(argv, preparedEventRequests, lambdaFacade.client)

        val counters = resubmit.counters
        counters.shouldNotBeNull()
        counters.errors shouldBe 0
        counters.recordCount shouldBeGreaterThan 0
    }
    
    private fun parseStoredEventBridgeEvent(
        objectContent: String,
        expectedEventId: String,
    ): JsonObject {
        return objectContent
            .lineSequence()
            .filter { it.isNotBlank() }
            .map { line -> json.parseToJsonElement(line).jsonObject }
            .first { eventBridgeEvent ->
                eventBridgeEvent.toString().contains(expectedEventId)
            }
    }

    private fun assertStoredFaultEventIsResubmittable(storedEvent: JsonObject) {
        val detail = storedEvent["detail"]?.jsonObject
            ?: error("Stored EventBridge event is missing required 'detail' object: $storedEvent")

        val type = detail["type"]?.jsonPrimitive?.content
            ?: error("Stored fault event detail is missing required 'type': $detail")
        type shouldBe "fault"

        val tags = detail["tags"]?.jsonObject
            ?: error("Stored fault event detail is missing required 'tags' object: $detail")

        val functionName = listOf("functionname", "functionName", "function_name")
            .firstNotNullOfOrNull { key -> tags[key]?.jsonPrimitive?.content }

        functionName.shouldNotBeNull()

        val uow = detail["uow"]?.jsonObject
            ?: error("Stored fault event detail is missing required 'uow' object: $detail")

        val record = uow["record"]?.jsonObject
        val batch = uow["batch"] as? JsonArray

        if (record == null) {
            batch.shouldNotBeNull()
            batch.size shouldBeGreaterThan 0

            val firstBatchRecord = batch[0].jsonObject["record"]
                ?: error("Stored fault event uow.batch[0] is missing required 'record': ${batch[0]}")

            firstBatchRecord.shouldNotBeNull()
        } else {
            record.shouldNotBeNull()
        }
    }

    @AfterAll
    fun tearDownAll() {
        eventBridgeFacade.close()
        lambdaFacade.close()
        s3Facade.close()
        snsFacade.close()
        sqsFacade.close()
    }
}