package io.github.huherto.awsLambdaStream.faults.replay

import kotlinx.serialization.Serializable

@Serializable
data class DynamoDbReplayRecord(
    val eventID: String? = null,
    val eventName: String? = null,
    val eventVersion: String? = null,
    val eventSource: String? = "aws:dynamodb",
    val eventSourceARN: String? = null,
    val awsRegion: String? = null,
    val dynamodb: DynamoDbStreamReplayData,
)

@Serializable
data class DynamoDbStreamReplayData(
    val approximateCreationDateTime: Double? = null,
    val keys: Map<String, DynamoDbAttributeValueSnapshot>? = null,
    val newImage: Map<String, DynamoDbAttributeValueSnapshot>? = null,
    val oldImage: Map<String, DynamoDbAttributeValueSnapshot>? = null,
    val sequenceNumber: String? = null,
    val sizeBytes: Long? = null,
    val streamViewType: String? = null,
)
