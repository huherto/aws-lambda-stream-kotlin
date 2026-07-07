package org.myorg.sut.facades

import aws.sdk.kotlin.services.kinesis.KinesisClient
import aws.sdk.kotlin.services.kinesis.model.GetRecordsRequest
import aws.sdk.kotlin.services.kinesis.model.GetShardIteratorRequest
import aws.sdk.kotlin.services.kinesis.model.ShardIteratorType
import kotlinx.coroutines.delay
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds

class KinesisFacade(
    private val config: AwsLocalConfig = AwsLocalConfig(),
    private val streamName: String = "sut-event-hub-local-s1",
) {
    val client: KinesisClient by lazy {
        KinesisClient {
            region = config.region
            endpointUrl = config.endpointUrl
            credentialsProvider = config.credentialsProvider()
        }
    }

    suspend fun <T> waitForResult(
        timeout: Duration = 20_000.milliseconds,
        delayBetweenAttempts: Duration = 1_000.milliseconds,
        onTimeout: () -> T?,
        block: suspend KinesisClient.() -> T?,
    ): T? {
        val startTime = System.currentTimeMillis()

        while (true) {
            val result = client.block()
            if (result != null) {
                return result
            }

            if (System.currentTimeMillis() - startTime > timeout.inWholeMilliseconds) {
                return onTimeout()
            }

            delay(delayBetweenAttempts)
        }
    }

    suspend fun readAllEvents(): List<String> {
        val events = mutableListOf<String>()

        val shardIteratorResponse = client.getShardIterator(GetShardIteratorRequest {
            this.streamName = this@KinesisFacade.streamName
            shardId = "shardId-000000000000"
            shardIteratorType = ShardIteratorType.TrimHorizon
        })

        var shardIterator = shardIteratorResponse.shardIterator

        while (shardIterator != null) {
            val recordsResponse = client.getRecords(GetRecordsRequest {
                this.shardIterator = shardIterator
            })

            recordsResponse.records.forEach { record ->
                events.add(record.data.decodeToString())
            }

            shardIterator = recordsResponse.nextShardIterator

            if (recordsResponse.records.isEmpty()) {
                break
            }
        }

        return events
    }

    fun close() {
        client.close()
    }
}