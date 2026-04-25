package org.myorg.sut

import aws.sdk.kotlin.runtime.auth.credentials.StaticCredentialsProvider
import aws.sdk.kotlin.services.dynamodb.DynamoDbClient
import aws.sdk.kotlin.services.dynamodb.model.AttributeValue
import aws.sdk.kotlin.services.dynamodb.model.QueryRequest
import aws.sdk.kotlin.services.dynamodb.model.QueryResponse
import aws.sdk.kotlin.services.eventbridge.EventBridgeClient
import aws.sdk.kotlin.services.eventbridge.model.PutEventsRequest
import aws.sdk.kotlin.services.eventbridge.model.PutEventsRequestEntry
import aws.sdk.kotlin.services.kinesis.KinesisClient
import aws.sdk.kotlin.services.kinesis.model.GetRecordsRequest
import aws.sdk.kotlin.services.kinesis.model.GetShardIteratorRequest
import aws.sdk.kotlin.services.kinesis.model.ShardIteratorType
import aws.smithy.kotlin.runtime.net.url.Url
import io.kotest.matchers.longs.shouldBeGreaterThan
import io.kotest.matchers.longs.shouldBeLessThan
import io.kotest.matchers.nulls.shouldNotBeNull
import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNotBe
import io.kotest.matchers.string.shouldMatch
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import kotlin.random.Random
import kotlin.time.Duration.Companion.milliseconds
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

        logger.info { "Sending event ${event.id}" }
        val res = eventBridgeClient.putEvents(PutEventsRequest {
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
        
        findEventByPK(event.id!!) { response ->
            val collectedEvent = response.items?.firstOrNull() ?: return@findEventByPK null

            logger.info { "Collected event id: ${event.id}" }
            logger.debug { "Collected event: $collectedEvent" }
            collectedEvent.shouldNotBeNull()
            collectedEvent["pk"]?.asS() shouldBe event.id
            collectedEvent["sk"]?.asS() shouldBe "EVENT"
            collectedEvent["discriminator"]?.asS() shouldBe "EVENT"
            collectedEvent["data"]?.asS() shouldBe event.entity?.id
            collectedEvent["event"]?.asS().shouldNotBeNull()

            val timeStamp = collectedEvent["timestamp"]?.asN()?.toLong()
            checkTimestampDiff(timeStamp, event.timestamp)

            collectedEvent
        }

        findEventByPK(event.entity?.id!!) { response ->
            logger.debug { "Checking correlated event among ${response.items?.size} events" }
            val correlEvent = response.items?.firstOrNull { rec -> rec["sk"]?.asS() == event.id }
            if (correlEvent == null) return@findEventByPK null

            logger.info { "Correlated event id: ${event.id}" }
            logger.debug { "Correlated event: $correlEvent" }
            correlEvent.shouldNotBeNull()
            correlEvent["pk"]?.asS() shouldBe event.entity?.id
            correlEvent["sk"]?.asS() shouldBe event.id
            correlEvent["discriminator"]?.asS() shouldBe "CORREL"
            correlEvent["expire"]?.asBool() shouldBe false
            correlEvent["awsregion"]?.asS() shouldBe "us-east-1"
            correlEvent["suffix"]?.asS() shouldBe ""
            correlEvent["pipelineId"]?.asS() shouldBe "corre1"

            val sequenceNumber = correlEvent["sequenceNumber"]?.asS()
            sequenceNumber.shouldNotBeNull()
            sequenceNumber shouldMatch "\\d+".toRegex()

            correlEvent["event"]?.asS().shouldNotBeNull()

            val ttl = correlEvent["ttl"]?.asN()?.toLong()
            ttl.shouldNotBeNull()
            ttl shouldBeGreaterThan 1742326911L // A date in 2025
            ttl shouldBeLessThan 1900093311L // A date in 2030

            val timeStamp = correlEvent["timestamp"]?.asN()?.toLong()
            checkTimestampDiff(timeStamp, event.timestamp)

            correlEvent
        }

        var vtaEventId : String? = null
        findEventByPK(event.entity?.id!!) { response ->
            val vtaEvent =
                response.items?.firstOrNull { rec -> rec["type"]?.asS() == "VERIFY_TARGET_ADDRESS" }
            if (vtaEvent == null) return@findEventByPK null

            vtaEvent.shouldNotBeNull()
            vtaEvent["partitionKey"] shouldBe event.entity?.id
            vtaEvent["type"] shouldBe "VERIFY_TARGET_ADDRESS"
            vtaEvent["data"] shouldBe event.entity?.id

            vtaEventId = vtaEvent["id"]?.asS()
            vtaEventId?.endsWith(".eval_vta") shouldBe true
            vtaEvent
        }

        vtaEventId.shouldNotBeNull()
        findEventByPK(vtaEventId!!) { response ->
            val vtaEvent = response.items?.firstOrNull() ?: return@findEventByPK null

            vtaEvent.shouldNotBeNull()
            vtaEvent["partitionKey"] shouldBe event.entity?.id
            vtaEvent["type"] shouldBe "VERIFY_TARGET_ADDRESS"
            vtaEvent["data"] shouldBe event.entity?.id
            vtaEvent
        }

        val kinesisEvents = readAllKinesisEvents()
        kinesisEvents shouldNotBe null
        logger.info { "Read ${kinesisEvents.size} events from Kinesis" }
        println("Read ${kinesisEvents.size} events from Kinesis")
        for (kinesisEvent in kinesisEvents) {
            logger.debug { "Kinesis event: $kinesisEvent" }
        }
        // kinesisEvents.size shouldBe 1
        //kinesisEvents[0].shouldMatch("\\{\"pk\":\"ship-\\d+\",\"sk\":\"EVENT\",\"discriminator\":\"EVENT\",\"data\":\"ship-\\d+\",\"event\":\"ShipmentCreatedEvent\"}".toRegex())
    }

    private fun checkTimestampDiff(t1: Long?, t2: Long?) {
        t1.shouldNotBeNull()
        t2.shouldNotBeNull()
        t1 shouldBeGreaterThan (t2 - 1L)
        t1 shouldBeLessThan (t2 + 100 * 1000L) // 1000 secs from now
    }

    @Test
    fun sendPoisonPillEvent() : Unit = runBlocking {

        val event = createPoisonPillEvent(createTrackedUnit())

        val res = eventBridgeClient.putEvents(PutEventsRequest {
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


    private suspend fun findEventByPK(pk: String, checkResponse: (QueryResponse)-> DBRecord?)  : DBRecord? {

        val startTime = System.currentTimeMillis()
        while (true) {
            if (System.currentTimeMillis() - startTime > 10000) {
                throw RuntimeException("Timed out waiting for event $pk to be inserted.")
            }
            val response = dynamoDbClient.query(QueryRequest {
                tableName = "sut-control-service-local-events"
                keyConditionExpression = "pk = :pk"
                expressionAttributeValues = mapOf(":pk" to AttributeValue.S(pk))
            })

            val found = checkResponse(response)
            if (found != null) {
                return found
            }
            delay(1000.milliseconds)
        }
    }

    private val eventBridgeClient: EventBridgeClient by lazy {
        EventBridgeClient {
            this.region = "us-east-1"
            this.endpointUrl = endPointUrl()
            credentialsProvider =
                StaticCredentialsProvider {
                    this.accessKeyId = "test"
                    this.secretAccessKey = "test"
                }
        }
    }

    private val kinesisClient: KinesisClient by lazy {
        KinesisClient {
            this.region = "us-east-1"
            this.endpointUrl = endPointUrl()
            credentialsProvider =
                StaticCredentialsProvider {
                    this.accessKeyId = "test"
                    this.secretAccessKey = "test"
                }
        }
    }

    private suspend fun readAllKinesisEvents(): List<String> {
        val streamName = "sut-event-hub-local-s1"
        val events = mutableListOf<String>()

        val shardIteratorResponse = kinesisClient.getShardIterator(GetShardIteratorRequest {
            this.streamName = streamName
            this.shardId = "shardId-000000000000"
            this.shardIteratorType = ShardIteratorType.TrimHorizon
        })

        var shardIterator = shardIteratorResponse.shardIterator

        while (shardIterator != null) {
            val recordsResponse = kinesisClient.getRecords(GetRecordsRequest {
                this.shardIterator = shardIterator
            })

            recordsResponse.records.forEach { record ->
                val data = record.data.decodeToString()
                events.add(data)
            }

            shardIterator = recordsResponse.nextShardIterator

            if (recordsResponse.records.isEmpty()) {
                break
            }
        }

        return events
    }


    private val dynamoDbClient: DynamoDbClient by lazy {
    DynamoDbClient {
            this.region = "us-east-1"
            this.endpointUrl = endPointUrl()
            credentialsProvider =
                StaticCredentialsProvider {
                    this.accessKeyId = "test"
                    this.secretAccessKey = "test"
                }
        }
    }

    @AfterAll
    fun tearDownAll() {
        eventBridgeClient.close()
        dynamoDbClient.close()
        kinesisClient.close()
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

    private fun createPoisonPillEvent(trackedUnit: TrackedUnit) = ShipmentCreatedEvent().apply {
        id = "poison-" + generateRandomNumber()
        partitionKey = trackedUnit.id
        timestamp = System.currentTimeMillis()
        location = "poison-pill"
        entity = trackedUnit
    }
}