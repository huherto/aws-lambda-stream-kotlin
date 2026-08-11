package io.github.huherto.awsLambdaStream.serialization.snapshots

import kotlinx.serialization.Serializable
import kotlinx.serialization.json.JsonObject

@Serializable
data class UnitOfWorkSnapshot(
    val pipeline: PipelineSnapshot? = null,
    val record: RecordSnapshot? = null,
    val event: EventSnapshot? = null,
    val key: String? = null,
    val sequenceNumber: String? = null,
    val shardId: String? = null,
    val timestamp: String? = null,
    val meta: Map<String, String?>? = null,
    val triggers: List<EventSnapshot>? = null,
    val correlated: List<EventSnapshot>? = null,
    val batch: List<UnitOfWorkSnapshot>? = null,
    val aws: List<AwsOperationSnapshot>? = null,
    val s3: S3Snapshot? = null,
    val extensions: Map<String, String?>? = null,
    val batchGetRequest: String? = null,
    val batchGetResponse: String? = null,
    val publishRequest: String? = null,
    val publishRequestEntry: String? = null,
    val publishResponse: String? = null,
    val putRequest: String? = null,
    val putResponse: String? = null,
    val queryParams: String? = null,
    val queryRequest: String? = null,
    val queryResponse: String? = null,
    val saveOptions: String? = null,
    val scanRequest: String? = null,
    val updateRequest: String? = null,
    val updateResponse: String? = null,
)

@Serializable
data class PipelineSnapshot(
    val id: String? = null,
)

@Serializable
data class RecordSnapshot(
    val kind: String,
    val payload: JsonObject,
    val diagnostic: RecordDiagnosticSnapshot? = null,
)

@Serializable
data class RecordDiagnosticSnapshot(
    val summary: String? = null,
    val fields: Map<String, String?>? = null,
)

@Serializable
data class AwsOperationSnapshot(
    val service: String? = null,
    val operation: String? = null,
    val requestId: String? = null,
    val statusCode: Int? = null,
)
