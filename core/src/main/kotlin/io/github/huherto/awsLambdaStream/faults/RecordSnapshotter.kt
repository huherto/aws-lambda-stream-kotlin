package io.github.huherto.awsLambdaStream.faults

interface RecordSnapshotter {
    fun supports(record: Any): Boolean

    fun snapshot(record: Any): ReplayRecordSnapshot
}
