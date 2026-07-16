package io.github.huherto.awsLambdaStream

import com.amazonaws.services.lambda.runtime.Context
import com.amazonaws.services.lambda.runtime.RequestHandler
import com.amazonaws.services.lambda.runtime.events.KinesisEvent
import com.amazonaws.services.lambda.runtime.events.StreamsEventResponse
import com.amazonaws.services.lambda.runtime.events.StreamsEventResponse.BatchItemFailure
import io.github.huherto.awsLambdaStream.from.KinesisAdapter
import io.github.huherto.awsLambdaStream.sinks.EventPublisherInMemory
import io.github.huherto.awsLambdaStream.testsupport.TestContext
import io.kotest.matchers.ints.shouldBeLessThan
import io.mockk.every
import io.mockk.spyk
import mu.KotlinLogging.logger
import org.junit.jupiter.api.Test
import java.nio.ByteBuffer
import kotlin.random.Random

/**
 * I wrote this class to experiment with the Kinesis batch stream. This is a summary of my findings.
 *
 * `reportBatchItemFailures` should not be set or set to false. The pipelines do not support reporting individual
 *  failures anyway.
 *
 * `bisectBatchOnError` should be set to true. The handler will split the batch in two, throttling the processing when
 *  the infrastructure is not stable.
 *
 * Regardless of any settings, events will be retried. Therefore, the operations on the events should be idempotent.
 *
 * If STREAM_RETRY_ENABLED is true, the fault manager will throw exceptions when there are retryable exceptions.
 * The exception will cause the batch to be submitted again, including events that have already been processed
 * successfully. The drawback is that more successful events will be retried.
 *
 * If STREAM_RETRY_ENABLED is false or not set, the fault manager will not throw exceptions. The failures will be
 * published to the event bus. The drawback is that retriable failures may end up as failures in the event bus.
 *
 * Regardless of any settings, if the event bus fails, then the handler will throw an exception and the batch
 * will be retried.
 *
 * DynamoDB and EventBridge connectors have their own retry logic with jitter and exponential backoff. This should
 * ameliorate the retries at the batch level problem.
 *
 * I thought about changing the pipeline to support reporting individual failures, but I didn't see a big gain since
 * the batch is retried anyway.  They just re-submit from the earliest failed sequence number. The idempotency
 * requirement remains.
 */
class KinesisBatchStreamTest {

    val log = logger {}

    /*
     * This handler simulates a Kinesis stream that processes events in batches.
     * It simulates a flaky infrastructure where some events fail to be processed.
     * The handler retries only the failed events and processes them successfully.
     */
    class KinesisBatchHandler : RequestHandler<KinesisEvent, StreamsEventResponse> {

        val log = logger {}

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
    class KinesisBatchHandlerWithPipeline : RequestHandler<KinesisEvent, StreamsEventResponse> {

        val log = logger {}

        val envConfig : EnvironmentConfig by lazy {
            val envConfig = spyk(EnvironmentConfig())
            every { envConfig.awsRegion() } returns "us-east-1"
            every { envConfig.streamRetryEnabled() } returns true
            envConfig
        }

        val faultManager = FaultManager(
            envConfig = envConfig,
            eventPublisher = EventPublisherInMemory()
        )

        val kinesisAdapter: KinesisAdapter by lazy {
            KinesisAdapter(
                faultManager = faultManager,
                eventCodec = MyEventCodec(),
            )
        }

        override fun handleRequest(event: KinesisEvent, context: Context): StreamsEventResponse {
            val failures = mutableListOf<BatchItemFailure>()
            val headFlow = kinesisAdapter.fromKinesis(event)
            with(faultManager) {
                // This is not really how a batch would be processed. There is no support
                // for reporting individual failures with pipelines.
                headFlow.mapNotFaulty { uow ->
                    try {
                        processRecord()
                    } catch (e: Exception) {
                        failures.add(StreamsEventResponse.BatchItemFailure(uow.sequenceNumber!!))
                    }
                }
            }

            // Return the response object to Lambda
            return StreamsEventResponse(failures)
        }

        private fun processRecord() {
            if (Random.nextInt(100) < 20) {
                throw RuntimeException("Error in processing record")
            }
        }
    }

    @Test
    fun `Kinesis batch stream with pipeline`() {
        retryBatchUntilAllEventsSucceed(KinesisBatchHandlerWithPipeline())
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
            val event = MyEventA().apply { id = i.toString() }
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