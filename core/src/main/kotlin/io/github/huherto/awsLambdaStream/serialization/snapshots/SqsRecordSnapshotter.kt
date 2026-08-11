package io.github.huherto.awsLambdaStream.serialization.snapshots

import com.amazonaws.services.lambda.runtime.events.SQSEvent
import io.github.huherto.awsLambdaStream.serialization.aws.SQSMessageReplayJson
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.jsonObject

class SqsRecordSnapshotter : RecordSnapshotter {
    override fun supports(record: Any): Boolean {
        return record is SQSEvent.SQSMessage
    }

    override fun snapshot(record: Any): RecordSnapshot {
        val sqsRecord = record as SQSEvent.SQSMessage
        val json = SQSMessageReplayJson.encode(sqsRecord)
        return RecordSnapshot(
            kind = "sqs",
            payload = Json.parseToJsonElement(json).jsonObject
        )
    }
}
