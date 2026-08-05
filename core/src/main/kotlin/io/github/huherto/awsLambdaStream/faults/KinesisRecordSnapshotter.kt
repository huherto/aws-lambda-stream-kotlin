package io.github.huherto.awsLambdaStream.faults

import com.amazonaws.services.lambda.runtime.events.KinesisEvent
import io.github.huherto.awsLambdaStream.faults.replay.KinesisReplayData
import io.github.huherto.awsLambdaStream.faults.replay.KinesisReplayRecord
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.encodeToJsonElement
import kotlinx.serialization.json.jsonObject
import java.nio.ByteBuffer
import java.util.*

class KinesisRecordSnapshotter : RecordSnapshotter {
    override fun supports(record: Any): Boolean {
        return record is KinesisEvent.KinesisEventRecord
    }

    override fun snapshot(record: Any): ReplayRecordSnapshot {
        val kinesisRecord = record as KinesisEvent.KinesisEventRecord

        val replayRecord = KinesisReplayRecord(
            eventID = kinesisRecord.eventID,
            eventName = kinesisRecord.eventName,
            eventSource = kinesisRecord.eventSource,
            eventSourceARN = kinesisRecord.eventSourceARN,
            awsRegion = kinesisRecord.awsRegion,
            kinesis = KinesisReplayData(
                partitionKey = kinesisRecord.kinesis.partitionKey,
                sequenceNumber = kinesisRecord.kinesis.sequenceNumber,
                data = encodeByteBuffer(kinesisRecord.kinesis.data),
                approximateArrivalTimestamp = kinesisRecord.kinesis.approximateArrivalTimestamp?.time?.toDouble()?.div(1000.0),
                kinesisSchemaVersion = kinesisRecord.kinesis.kinesisSchemaVersion
            )
        )

        return ReplayRecordSnapshot(
            kind = "kinesis",
            payload = Json.encodeToJsonElement(replayRecord).jsonObject
        )
    }

    private fun encodeByteBuffer(bb: ByteBuffer): String {
        val duplicate = bb.duplicate()
        val bytes = ByteArray(duplicate.remaining())
        duplicate.get(bytes)
        return Base64.getEncoder().encodeToString(bytes)
    }
}
