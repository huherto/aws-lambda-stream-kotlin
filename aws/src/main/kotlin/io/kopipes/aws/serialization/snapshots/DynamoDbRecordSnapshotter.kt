package io.kopipes.aws.serialization.snapshots

import com.amazonaws.services.lambda.runtime.events.DynamodbEvent
import io.kopipes.aws.serialization.aws.DynamodbStreamRecordReplayJson
import io.kopipes.core.serialization.snapshots.RecordSnapshot
import io.kopipes.core.serialization.snapshots.RecordSnapshotter
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.jsonObject

class DynamoDbRecordSnapshotter : RecordSnapshotter {
    override fun supports(record: Any): Boolean {
        return record is DynamodbEvent.DynamodbStreamRecord
    }

    override fun snapshot(record: Any): RecordSnapshot {
        val dynamodbRecord = record as DynamodbEvent.DynamodbStreamRecord
        val json = DynamodbStreamRecordReplayJson.encode(dynamodbRecord)
        return RecordSnapshot(
            kind = "dynamodb",
            payload = Json.parseToJsonElement(json).jsonObject
        )
    }
}
