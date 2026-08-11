package io.github.huherto.awsLambdaStream.serialization.snapshots

import com.amazonaws.services.lambda.runtime.events.DynamodbEvent
import io.github.huherto.awsLambdaStream.serialization.aws.DynamodbStreamRecordReplayJson
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
