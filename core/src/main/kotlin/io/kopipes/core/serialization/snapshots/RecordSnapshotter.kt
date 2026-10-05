package io.kopipes.core.serialization.snapshots

interface RecordSnapshotter {
    fun supports(record: Any): Boolean
    fun snapshot(record: Any): RecordSnapshot
}
