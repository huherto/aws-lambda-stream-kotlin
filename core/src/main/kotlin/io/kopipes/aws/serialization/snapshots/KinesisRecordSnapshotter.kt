package io.kopipes.aws.serialization.snapshots

import com.amazonaws.services.lambda.runtime.events.KinesisEvent
import io.kopipes.aws.serialization.aws.KinesisEventRecordReplayJson
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.jsonObject

class KinesisRecordSnapshotter : RecordSnapshotter {
    override fun supports(record: Any): Boolean {
        return record is KinesisEvent.KinesisEventRecord
    }

    override fun snapshot(record: Any): RecordSnapshot {
        val kinesisRecord = record as KinesisEvent.KinesisEventRecord
        val json = KinesisEventRecordReplayJson.encode(kinesisRecord)
        return RecordSnapshot(
            kind = "kinesis",
            payload = Json.parseToJsonElement(json).jsonObject
        )
    }
}
