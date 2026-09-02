package io.github.huherto.awsLambdaStream

import aws.smithy.kotlin.runtime.ErrorMetadata
import aws.smithy.kotlin.runtime.InternalApi
import aws.smithy.kotlin.runtime.SdkBaseException
import aws.smithy.kotlin.runtime.ServiceErrorMetadata
import com.amazonaws.services.lambda.runtime.Context
import com.amazonaws.services.lambda.runtime.RequestHandler
import com.amazonaws.services.lambda.runtime.events.KinesisEvent
import com.amazonaws.services.lambda.runtime.events.StreamsEventResponse
import com.amazonaws.services.lambda.runtime.events.StreamsEventResponse.BatchItemFailure
import io.github.huherto.awsLambdaStream.faults.FaultManager
import io.github.huherto.awsLambdaStream.from.KinesisAdapter
import io.github.huherto.awsLambdaStream.sinks.EventPublisherInMemory
import io.github.huherto.awsLambdaStream.testsupport.TestContext
import io.kotest.matchers.ints.shouldBeLessThan
import io.mockk.every
import io.mockk.spyk
import kotlinx.coroutines.runBlocking
import mu.KotlinLogging.logger
import org.junit.jupiter.api.Test
import java.nio.ByteBuffer
import kotlin.random.Random

/** Tests for Kinesis batch stream processing. */
class KinesisBatchStreamTest {

    val log = logger {}

    /*
     * This handler simulates a Kinesis stream that processes events in batches.
     * It simulates a flaky infrastructure where some events fail to be processed.
     * The handler retries only the failed events and processes them successfully.
     */
    /** Simulated Kinesis batch handler. */
    class KinesisBatchHandler : RequestHandler<KinesisEvent, StreamsEventResponse> {

        override fun handleRequest(event: KinesisEvent, context: Context): StreamsEventResponse {
            val failures = mutableListOf<BatchItemFailure>()

            for (record in event.records) {
                val sequenceNumber = record.kinesis.sequenceNumber
                try {
                    processRecord()
                } catch (e: Exception) {
                    failures.add(StreamsEventResponse.BatchItemFailure(sequenceNumber))
                }
            }
            // Return the response object to Lambda
            return StreamsEventResponse(failures)
        }

        private fun processRecord() {
            if (Random.nextInt(100) < 50) {
                throw RuntimeException("Error in processing record")
            }
        }
    }

    // This was an attempt to use a pipeline to process the records.
    // It was not successful because there is no support for reporting individual failures with pipelines.
    /** Simulated Kinesis batch handler with pipeline. */
    class KinesisBatchHandlerWithPipeline : RequestHandler<KinesisEvent, StreamsEventResponse> {

        val log = logger {}

        val envConfig : EnvironmentConfig by lazy {
            val envConfig = spyk(EnvironmentConfig())
            every { envConfig.awsRegion() } returns "us-east-1"
            every { envConfig.streamRetryEnabled() } returns true
            envConfig
        }

        val faultManager = FaultManager(
            eventPublisher = EventPublisherInMemory(),
            skipErrorLogging = true,
            isItemLevelRetryEnabled = true,
        )

        val kinesisAdapter: KinesisAdapter by lazy {
            KinesisAdapter(
                faultManager = faultManager,
                eventCodec = MyEventCodec(),
            )
        }

        override fun handleRequest(event: KinesisEvent, context: Context): StreamsEventResponse = runBlocking {
            val headFlow = kinesisAdapter.fromKinesis(event)
            with(faultManager) {
                headFlow.mapNotFaulty {
                    processRecord()
                }
                .collect() {}
            }

            return@runBlocking StreamsEventResponse(faultManager.kinesisRetryableFailures())
        }

        /** Dummy retryable exception. */
        class DummyRetryableException(message: String) : SdkBaseException(message) {
            @OptIn(InternalApi::class)
            override val sdkErrorMetadata: ErrorMetadata = ServiceErrorMetadata().apply {
                attributes[ErrorMetadata.Retryable] = true
            }
        }

        private fun processRecord() {
            if (Random.nextInt(100) < 50) {
                throw DummyRetryableException("Error in processing record")
            }
        }
    }

    @Test
    fun `Kinesis batch stream with pipeline`() {
        val numAttempts = retryBatchUntilAllEventsSucceed(KinesisBatchHandlerWithPipeline())
        numAttempts shouldBeLessThan 50
    }

    @Test
    fun `Kinesis batch stream without pipeline`() {
        val numAttempts = retryBatchUntilAllEventsSucceed(KinesisBatchHandler())
        numAttempts shouldBeLessThan 50
    }

    fun retryBatchUntilAllEventsSucceed(handler:  RequestHandler<KinesisEvent, StreamsEventResponse>): Int {

        val context = TestContext()
        val map = createEventMap(1000)
        val event = KinesisEvent()
        event.records = map.values.toList()

        for(attemptNumber in 1..100) {
            val response = handler.handleRequest(event, context)
            log.warn{"Attempt $attemptNumber: records failed: ${response.batchItemFailures.size}"}
            if (response.batchItemFailures.isEmpty()) {
                return attemptNumber
            }
            val records = response.batchItemFailures.map {
                map[it.itemIdentifier]!!
            }.toList()
            event.records.clear()
            event.records.addAll(records)
        }
        return 101
    }

    private fun createEventMap(numEvents: Int): MutableMap<String, KinesisEvent.KinesisEventRecord> {
        val map = mutableMapOf<String, KinesisEvent.KinesisEventRecord>()
        val eventCodec = MyEventCodec()
        for (i in 1..numEvents) {
            val event = MyEventA(id = i.toString())
            val eventAsString = eventCodec.encode(event)
            val eventRecord = KinesisEvent.KinesisEventRecord()
            eventRecord.kinesis = KinesisEvent.Record().apply {
                sequenceNumber = Random.Default.nextInt(10000).toString().padStart(5, '0')
                data = ByteBuffer.wrap(eventAsString.toByteArray(Charsets.UTF_8))
            }
            map[eventRecord.kinesis.sequenceNumber] = eventRecord
        }
        return map
    }

}