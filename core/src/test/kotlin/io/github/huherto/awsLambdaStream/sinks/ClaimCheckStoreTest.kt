package io.github.huherto.awsLambdaStream.sinks

import aws.sdk.kotlin.services.s3.S3Client
import aws.sdk.kotlin.services.s3.model.PutObjectRequest
import aws.sdk.kotlin.services.s3.model.PutObjectResponse
import aws.smithy.kotlin.runtime.content.toByteArray
import io.github.huherto.awsLambdaStream.EnvironmentConfig
import io.github.huherto.awsLambdaStream.FaultManager
import io.github.huherto.awsLambdaStream.MyEventA
import io.github.huherto.awsLambdaStream.UnitOfWork
import io.github.huherto.awsLambdaStream.connectors.S3ClientFactory
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.shouldBe
import io.kotest.matchers.string.shouldContain
import io.mockk.*
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test
import java.time.Clock
import java.time.Instant
import java.time.ZoneOffset

class ClaimCheckStoreTest {

    private val envConfig = spyk<EnvironmentConfig>()
    private val s3Client = mockk<S3Client>()
    private val s3ClientFactory = RecordingS3ClientFactory(s3Client)
    private val faultManager = FaultManager(
        envConfig = envConfig,
        eventPublisher = EventPublisherInMemory(),
        skipErrorLogging = true,
    )
    private val clock = Clock.fixed(Instant.parse("2024-03-05T06:07:08Z"), ZoneOffset.UTC)

    @Test
    fun `formatKey uses region clock and event id`() {
        // arrange
        val store = claimCheckStore()
        val event = MyEventA().apply { id = "event-123" }

        every { envConfig.awsRegion() } returns "us-west-2"

        // act
        val key = store.formatKey(event)

        // assert
        key shouldBe "us-west-2/claimchecks/2024/03/05/06/event-123"
    }

    @Test
    fun `formatKey fails when event id is missing`() {
        // arrange
        val store = claimCheckStore()
        val event = MyEventA()

        every { envConfig.awsRegion() } returns "us-west-2"

        // act
        val error = shouldThrow<IllegalStateException> {
            store.formatKey(event)
        }

        // assert
        error.message shouldBe "Cannot create claim-check key for event without id"
    }

    @Test
    fun `toClaimCheckEvent keeps event metadata and points to s3 object`() {
        // arrange
        val store = claimCheckStore()
        val event = MyEventA(foo = "foo", bar = "bar").apply {
            id = "event-123"
            partitionKey = "partition-1"
            timestamp = 123456789L
            tags = mapOf("source" to "test")
        }

        every { envConfig.awsRegion() } returns "eu-central-1"

        // act
        val claimCheckEvent = store.toClaimCheckEvent(event, "claim-check-bucket")

        // assert
        claimCheckEvent.id shouldBe "event-123"
        claimCheckEvent.eventType() shouldBe "MY_EVENT_A"
        claimCheckEvent.partitionKey shouldBe "partition-1"
        claimCheckEvent.timestamp shouldBe 123456789L
        claimCheckEvent.tags shouldBe mapOf("source" to "test")
        claimCheckEvent.s3 shouldBe ClaimCheckStore.ClaimCheck(
            bucket = "claim-check-bucket",
            key = "eu-central-1/claimchecks/2024/03/05/06/event-123",
        )
        claimCheckEvent.encoded() shouldContain "\"bucket\": \"claim-check-bucket\""
        claimCheckEvent.encoded() shouldContain "\"key\": \"eu-central-1/claimchecks/2024/03/05/06/event-123\""
    }

    @Test
    fun `toPutClaimCheckRequest creates request with bucket key and encoded event body`() = runTest {
        // arrange
        val store = claimCheckStore()
        val event = MyEventA(foo = "foo", bar = "bar").apply { id = "event-123" }

        every { envConfig.awsRegion() } returns "ap-southeast-2"

        // act
        val request = store.toPutClaimCheckRequest(event, "claim-check-bucket")

        // assert
        request.bucket shouldBe "claim-check-bucket"
        request.key shouldBe "ap-southeast-2/claimchecks/2024/03/05/06/event-123"
        request.body?.toByteArray()?.decodeToString() shouldBe event.encoded()
    }

    @Test
    fun `storeClaimCheck returns original flow when bucket is not configured`() = runTest {
        // arrange
        val storeWithoutBucket = claimCheckStore(bucket = " ")
        val uows = listOf(
            UnitOfWork(key = "one", event = MyEventA().apply { id = "event-1" }),
            UnitOfWork(key = "two", event = MyEventA().apply { id = "event-2" }),
        )

        // act
        val result = storeWithoutBucket.storeClaimCheck(uows.asFlow()).toList()

        // assert
        result shouldBe uows
        s3ClientFactory.requestedPipelineIds shouldBe emptyList()
        confirmVerified(s3Client)
    }

    @Test
    fun `storeClaimCheck stores single event and leaves eventless unit of work unchanged`() = runTest {
        // arrange
        val store = claimCheckStore()
        val response = PutObjectResponse {}
        val event = MyEventA(foo = "foo").apply { id = "event-123" }
        val eventUow = UnitOfWork(key = "with-event", event = event)
        val eventlessUow = UnitOfWork(key = "without-event")

        every { envConfig.awsRegion() } returns "us-east-1"
        coEvery { s3Client.putObject(any<PutObjectRequest>()) } returns response

        // act
        val result = store.storeClaimCheck(flowOf(eventUow, eventlessUow)).toList()

        // assert
        result shouldHaveSize 2

        val storedUow = result[0]
        val claimCheckEvent = storedUow.event as ClaimCheckStore.ClaimCheckEvent
        claimCheckEvent.id shouldBe "event-123"
        claimCheckEvent.eventType() shouldBe "MY_EVENT_A"
        claimCheckEvent.s3.bucket shouldBe "claim-check-bucket"
        claimCheckEvent.s3.key shouldBe "us-east-1/claimchecks/2024/03/05/06/event-123"
        storedUow.s3.putRequest?.bucket shouldBe "claim-check-bucket"
        storedUow.s3.putRequest?.key shouldBe "us-east-1/claimchecks/2024/03/05/06/event-123"
        storedUow.s3.putResponse shouldBe response

        result[1] shouldBe eventlessUow
        s3ClientFactory.requestedPipelineIds shouldBe listOf("unknown")
        coVerify(exactly = 1) {
            s3Client.putObject(
                match<PutObjectRequest> {
                    it.bucket == "claim-check-bucket" &&
                        it.key == "us-east-1/claimchecks/2024/03/05/06/event-123"
                }
            )
        }
        confirmVerified(s3Client)
    }

    @Test
    fun `storeClaimCheck stores every event inside a batch and preserves batch wrapper`() = runTest {
        // arrange
        val store = claimCheckStore()
        val response1 = PutObjectResponse { eTag = "etag-1" }
        val response2 = PutObjectResponse { eTag = "etag-2" }
        val event1 = MyEventA(foo = "foo-1").apply { id = "event-1" }
        val event2 = MyEventA(foo = "foo-2").apply { id = "event-2" }
        val batchWrapper = UnitOfWork(
            key = "batch-wrapper",
            batch = listOf(
                UnitOfWork(key = "batch-item-1", event = event1),
                UnitOfWork(key = "batch-item-2", event = event2),
            ),
        )

        every { envConfig.awsRegion() } returns "us-east-1"
        coEvery {
            s3Client.putObject(match<PutObjectRequest> { it.key?.endsWith("/event-1") == true })
        } returns response1
        coEvery {
            s3Client.putObject(match<PutObjectRequest> { it.key?.endsWith("/event-2") == true })
        } returns response2

        // act
        val result = store.storeClaimCheck(flowOf(batchWrapper)).toList()

        // assert
        result shouldHaveSize 1
        result[0].key shouldBe "batch-wrapper"
        result[0].batch?.shouldHaveSize(2)

        val firstItem = result[0].batch!![0]
        val firstClaimCheckEvent = firstItem.event as ClaimCheckStore.ClaimCheckEvent
        firstClaimCheckEvent.s3.key shouldBe "us-east-1/claimchecks/2024/03/05/06/event-1"
        firstItem.s3.putRequest?.key shouldBe "us-east-1/claimchecks/2024/03/05/06/event-1"
        firstItem.s3.putResponse shouldBe response1

        val secondItem = result[0].batch!![1]
        val secondClaimCheckEvent = secondItem.event as ClaimCheckStore.ClaimCheckEvent
        secondClaimCheckEvent.s3.key shouldBe "us-east-1/claimchecks/2024/03/05/06/event-2"
        secondItem.s3.putRequest?.key shouldBe "us-east-1/claimchecks/2024/03/05/06/event-2"
        secondItem.s3.putResponse shouldBe response2

        s3ClientFactory.requestedPipelineIds shouldBe listOf("unknown", "unknown")
        coVerify(exactly = 1) {
            s3Client.putObject(
                match<PutObjectRequest> {
                    it.bucket == "claim-check-bucket" &&
                        it.key == "us-east-1/claimchecks/2024/03/05/06/event-1"
                }
            )
        }
        coVerify(exactly = 1) {
            s3Client.putObject(
                match<PutObjectRequest> {
                    it.bucket == "claim-check-bucket" &&
                        it.key == "us-east-1/claimchecks/2024/03/05/06/event-2"
                }
            )
        }
        confirmVerified(s3Client)
    }

    private fun claimCheckStore(bucket: String? = "claim-check-bucket"): ClaimCheckStore {
        return ClaimCheckStore(
            envConfig = envConfig,
            s3ClientFactory = s3ClientFactory,
            faultManager = faultManager,
            claimCheckBucketName = bucket,
            clock = clock,
            bufferCapacity = 1,
        )
    }

    private class RecordingS3ClientFactory(
        private val client: S3Client,
    ) : S3ClientFactory {
        val requestedPipelineIds = mutableListOf<String>()

        override fun getClient(pipelineId: String): S3Client {
            requestedPipelineIds += pipelineId
            return client
        }
    }
}