package org.myorg.sut.facades

import aws.sdk.kotlin.services.kinesis.KinesisClient
import aws.sdk.kotlin.services.kinesis.model.GetRecordsRequest
import aws.sdk.kotlin.services.kinesis.model.GetShardIteratorRequest
import aws.sdk.kotlin.services.kinesis.model.ShardIteratorType

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