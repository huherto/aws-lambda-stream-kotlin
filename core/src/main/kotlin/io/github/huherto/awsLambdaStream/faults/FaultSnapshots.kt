package io.github.huherto.awsLambdaStream.faults

import kotlinx.serialization.Serializable
import kotlinx.serialization.json.JsonObject

@Serializable
data class ErrorSnapshot(
    val name: String? = null,
    val message: String? = null,
    val stackTrace: List<String>? = null,
)

@Serializable
data class PipelineSnapshot(
    val id: String? = null,
)

@Serializable
data class EventSummarySnapshot(
    val id: String? = null,
    val type: String? = null,
    val timestamp: Long? = null,
    val partitionKey: String? = null,
    val tags: Map<String, String>? = null,
)

@Serializable
data class RecordDiagnosticSnapshot(
    val summary: String? = null,
    val fields: Map<String, String?>? = null,
)

@Serializable
data class ReplayRecordSnapshot(
    val kind: String,
    val payload: JsonObject,
    val diagnostic: RecordDiagnosticSnapshot? = null,
)

@Serializable
data class AwsOperationSnapshot(
    val service: String? = null,
    val operation: String? = null,
    val requestId: String? = null,
    val statusCode: Int? = null,
)

@Serializable
data class S3Snapshot(
    val bucket: String? = null,
    val key: String? = null,
)

@Serializable
data class UnitOfWorkSnapshot(
    val pipeline: PipelineSnapshot? = null,
    val record: ReplayRecordSnapshot? = null,
    val event: EventSummarySnapshot? = null,
    val key: String? = null,
    val sequenceNumber: String? = null,
    val shardId: String? = null,
    val timestamp: String? = null,
    val meta: Map<String, String?>? = null,
    val triggers: List<EventSummarySnapshot>? = null,
    val correlated: List<EventSummarySnapshot>? = null,
    val batch: List<UnitOfWorkSnapshot>? = null,
    val aws: List<AwsOperationSnapshot>? = null,
    val s3: S3Snapshot? = null,
)
