package io.github.huherto.awsLambdaStream.faults.replay

import kotlinx.serialization.Serializable

@Serializable
data class KinesisReplayRecord(
    val eventID: String? = null,
    val eventName: String? = null,
    val eventSource: String? = "aws:kinesis",
    val eventSourceARN: String? = null,
    val awsRegion: String? = null,
    val kinesis: KinesisReplayData,
)

@Serializable
data class KinesisReplayData(
    val partitionKey: String? = null,
    val sequenceNumber: String? = null,
    val data: String? = null,
    val approximateArrivalTimestamp: Double? = null,
    val kinesisSchemaVersion: String? = null,
)
