package io.github.huherto.awsLambdaStream.serialization.snapshots

interface RecordSnapshotter {
    fun supports(record: Any): Boolean
    fun snapshot(record: Any): RecordSnapshot
}
