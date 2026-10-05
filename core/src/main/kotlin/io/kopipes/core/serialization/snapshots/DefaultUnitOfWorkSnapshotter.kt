package io.kopipes.core.serialization.snapshots

import io.kopipes.core.RawRecord
import io.kopipes.core.UnitOfWork
import io.kopipes.core.serialization.Snapshottable
import kotlinx.serialization.json.*

class DefaultUnitOfWorkSnapshotter(
    private val recordSnapshotters: List<RecordSnapshotter> = io.kopipes.core.GlobalRegistry.registeredRecordSnapshotters(),
    private val awsOperationCapturer: ((UnitOfWork) -> List<AwsOperationSnapshot>?)? = null,
) : UnitOfWorkSnapshotter {
    override fun snapshot(uow: UnitOfWork): UnitOfWorkSnapshot {
        return UnitOfWorkSnapshot(
            pipeline = uow.pipeline?.let { PipelineSnapshot(id = it.id) },
            record = uow.record?.let { record ->
                recordSnapshotters.find { it.supports(record) }?.snapshot(record)
                    ?: RecordSnapshot(kind = "unknown", payload = JsonObject(emptyMap()))
            },
            event = uow.event?.let { event ->
                EventSnapshot(
                    id = event.id,
                    type = event.eventType(),
                    timestamp = event.timestamp,
                    partitionKey = event.partitionKey,
                    tags = event.tags,
                    raw = event.raw?.let { Json.encodeToString(RawRecord.serializer(), it) },
                    eem = event.eem?.toString(),
                    triggers = event.triggers,
                )
            },
            key = uow.key,
            sequenceNumber = uow.sequenceNumber,
            shardId = uow.shardId,
            timestamp = uow.timestamp,
            meta = uow.meta,
            triggers = uow.triggers?.map { event ->
                EventSnapshot(
                    id = event.id,
                    type = event.eventType(),
                    timestamp = event.timestamp,
                    partitionKey = event.partitionKey,
                    tags = event.tags
                )
            },
            correlated = uow.correlated?.map { event ->
                EventSnapshot(
                    id = event.id,
                    type = event.eventType(),
                    timestamp = event.timestamp,
                    partitionKey = event.partitionKey,
                    tags = event.tags
                )
            },
            batch = uow.batch?.map { snapshot(it) },
            aws = awsOperationCapturer?.invoke(uow),
            extensions = snapshotExtensions(uow),
        )
    }

    private fun snapshotExtensions(uow: UnitOfWork): Map<String, JsonElement>? {
        return uow.extensions.entries
            .associate { (k, v) ->
                val name = k.simpleName ?: "unknown"
                val snapshot = if (v is Snapshottable) v.toSnapshot() else v
                name to snapshot.toJsonElement()
            }
            .filterValues { it !is JsonNull }
            .takeUnless { it.isEmpty() }
    }

    private fun Any?.toJsonElement(): JsonElement {
        return when (this) {
            null -> JsonNull
            is JsonElement -> this
            is String -> JsonPrimitive(toSnapshotString())
            is Number -> JsonPrimitive(this)
            is Boolean -> JsonPrimitive(this)
            is ByteArray -> JsonPrimitive(decodeToString())
            is Map<*, *> -> JsonObject(
                entries.associate { (key, value) ->
                    key.toString() to value.toJsonElement()
                }
            )
            is Iterable<*> -> JsonArray(map { it.toJsonElement() })
            is Array<*> -> JsonArray(map { it.toJsonElement() })
            else -> JsonPrimitive(toString().toSnapshotString())
        }
    }

    private fun String.toSnapshotString(): String {
        return trim().removeSurrounding("\"")
    }
}
