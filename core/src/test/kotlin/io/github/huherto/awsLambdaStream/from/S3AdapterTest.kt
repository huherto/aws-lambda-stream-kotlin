package io.github.huherto.awsLambdaStream.from

import com.amazonaws.services.lambda.runtime.events.SQSEvent
import io.github.huherto.awsLambdaStream.MyEventA
import io.github.huherto.awsLambdaStream.MyEventCodec
import io.github.huherto.awsLambdaStream.connectors.S3Connector
import io.github.huherto.awsLambdaStream.faults.FaultManager
import io.github.huherto.awsLambdaStream.sinks.EventPublisherInMemory
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNotBe
import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.mockkConstructor
import io.mockk.unmockkAll
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test

class S3AdapterTest {

    private val faultManager = FaultManager(
        eventPublisher = EventPublisherInMemory(),
        skipErrorLogging = true,
    )

    private val eventCodec = MyEventCodec()

    private val adapter = S3Adapter(
        faultManager = faultManager,
        eventCodec = eventCodec,
    )

    @AfterEach
    fun tearDown() {
        unmockkAll()
    }

    @Test
    fun `fromS3 should map S3Records to UnitOfWork`() = runTest {
        // Arrange
        val s3Event = S3Adapter.S3Event(
            records = listOf(
                S3Adapter.S3Record(
                    s3 = S3Adapter.S3Entity(
                        bucket = S3Adapter.S3Bucket(name = "test-bucket"),
                        `object` = S3Adapter.S3Object(key = "test-key-1")
                    )
                ),
                S3Adapter.S3Record(
                    s3 = S3Adapter.S3Entity(
                        bucket = S3Adapter.S3Bucket(name = "test-bucket"),
                        `object` = S3Adapter.S3Object(key = "test-key-2")
                    )
                )
            )
        )

        // Act
        val results = adapter.fromS3(s3Event).toList()

        // Assert
        results.shouldHaveSize(2)
        (results[0].record as S3Adapter.S3Record).s3.`object`.key shouldBe "test-key-1"
        (results[1].record as S3Adapter.S3Record).s3.`object`.key shouldBe "test-key-2"
    }

    @Test
    fun `fromSqsSnsS3 should decode SQS-SNS-S3 chain`() = runTest {
        // Arrange
        val s3Notification = S3Adapter.S3Notification(
            bucket = S3Adapter.S3Bucket(name = "test-bucket"),
            `object` = S3Adapter.S3Object(key = "test-key")
        )
        val sqsEvent = createSqsSnsS3Event(listOf(s3Notification))

        // Act
        val results = adapter.fromSqsSnsS3(sqsEvent).toList()

        // Assert
        results.shouldHaveSize(1)
        val s3Record = results[0].record as S3Adapter.S3Record
        s3Record.s3.bucket.name shouldBe "test-bucket"
        s3Record.s3.`object`.key shouldBe "test-key"
    }

    @Test
    fun `fromS3Event should fetch object from S3 and decode it`() = runTest {
        // Arrange
        mockkConstructor(S3Connector::class)
        val eventContent = """{"type":"MY_EVENT_A","foo":"bar"}"""
        coEvery { anyConstructed<S3Connector>().getObjectAsText(any(), any()) } returns eventContent

        val s3Notification = S3Adapter.S3Notification(
            bucket = S3Adapter.S3Bucket(name = "test-bucket"),
            `object` = S3Adapter.S3Object(key = "test-key")
        )
        val sqsEvent = createSqsSnsS3Event(listOf(s3Notification))

        // Act
        val results = adapter.fromS3Event(sqsEvent).toList()

        // Assert
        results.shouldHaveSize(1)
        val uow = results[0]
        uow.event shouldBe MyEventA(foo = "bar")
        
        coVerify { 
            anyConstructed<S3Connector>().getObjectAsText(
                match { it.bucket == "test-bucket" && it.key == "test-key" },
                any()
            )
        }
    }

    @Test
    fun `fromS3Event should handle S3 fetch failures`() = runTest {
        // Arrange
        mockkConstructor(S3Connector::class)
        coEvery { anyConstructed<S3Connector>().getObjectAsText(any(), any()) } throws Exception("S3 error")

        val s3Notification = S3Adapter.S3Notification(
            bucket = S3Adapter.S3Bucket(name = "test-bucket"),
            `object` = S3Adapter.S3Object(key = "test-key")
        )
        val sqsEvent = createSqsSnsS3Event(listOf(s3Notification))

        // Act
        val results = adapter.fromS3Event(sqsEvent).toList()

        // Assert
        results.shouldHaveSize(0)
        val faults = faultManager.getFaults()
        faults.shouldHaveSize(1)
        faults[0].err?.message shouldBe "java.lang.Exception: S3 error"
    }

    @Test
    fun `fromS3Event should handle decode failures`() = runTest {
        // Arrange
        mockkConstructor(S3Connector::class)
        coEvery { anyConstructed<S3Connector>().getObjectAsText(any(), any()) } returns """{"type":"UNKNOWN"}"""

        val s3Notification = S3Adapter.S3Notification(
            bucket = S3Adapter.S3Bucket(name = "test-bucket"),
            `object` = S3Adapter.S3Object(key = "test-key")
        )
        val sqsEvent = createSqsSnsS3Event(listOf(s3Notification))

        // Act
        val results = adapter.fromS3Event(sqsEvent).toList()

        // Assert
        results.shouldHaveSize(0)
        val faults = faultManager.getFaults()
        faults.shouldHaveSize(1)
        // MyEventCodec probably throws an exception for unknown types or returns something else.
        // Let's assume it fails.
    }

    @Test
    fun `fromS3Event should process multiple records in one SQS message`() = runTest {
        // Arrange
        mockkConstructor(S3Connector::class)
        coEvery { anyConstructed<S3Connector>().getObjectAsText(any(), any()) } returnsMany listOf(
            """{"type":"MY_EVENT_A","foo":"bar-1"}""",
            """{"type":"MY_EVENT_A","foo":"bar-2"}"""
        )

        val notifications = listOf(
            S3Adapter.S3Notification(
                bucket = S3Adapter.S3Bucket(name = "test-bucket"),
                `object` = S3Adapter.S3Object(key = "test-key-1")
            ),
            S3Adapter.S3Notification(
                bucket = S3Adapter.S3Bucket(name = "test-bucket"),
                `object` = S3Adapter.S3Object(key = "test-key-2")
            )
        )
        val sqsEvent = createSqsSnsS3Event(notifications)

        // Act
        val results = adapter.fromS3Event(sqsEvent).toList()

        // Assert
        results.shouldHaveSize(2)
        results[0].event shouldBe MyEventA(foo = "bar-1")
        results[1].event shouldBe MyEventA(foo = "bar-2")
    }

    @Test
    fun `toS3Records should create S3Event from notifications`() {
        // Arrange
        val notifications = listOf(
            S3Adapter.S3Notification(
                bucket = S3Adapter.S3Bucket(name = "bucket-1"),
                `object` = S3Adapter.S3Object(key = "key-1")
            )
        )

        // Act
        val result = adapter.toS3Records(notifications)

        // Assert
        result.records.shouldHaveSize(1)
        result.records[0].s3.bucket.name shouldBe "bucket-1"
        result.records[0].s3.`object`.key shouldBe "key-1"
        result.records[0].eventSource shouldBe "aws:s3"
    }

    @Test
    fun `toSqsSnsS3Records should create SqsEvent from notifications`() {
        // Arrange
        val notifications = listOf(
            S3Adapter.S3Notification(
                bucket = S3Adapter.S3Bucket(name = "bucket-1"),
                `object` = S3Adapter.S3Object(key = "key-1")
            )
        )

        // Act
        val result = adapter.toSqsSnsS3Records(notifications)

        // Assert
        result.records.shouldHaveSize(1)
        result.records[0].body.shouldNotBe(null)
    }

    private fun createSqsSnsS3Event(notifications: List<S3Adapter.S3Notification>): SQSEvent {
        val sqsEvent = adapter.toSqsSnsS3Records(notifications)
        return SQSEvent().apply {
            records = sqsEvent.records.map {
                SQSEvent.SQSMessage().apply {
                    body = it.body
                }
            }
        }
    }
}
