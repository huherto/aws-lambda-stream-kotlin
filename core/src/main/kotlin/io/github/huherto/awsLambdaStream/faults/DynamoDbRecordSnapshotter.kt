package io.github.huherto.awsLambdaStream.faults

import com.amazonaws.services.lambda.runtime.events.DynamodbEvent
import com.amazonaws.services.lambda.runtime.events.models.dynamodb.AttributeValue
import io.github.huherto.awsLambdaStream.faults.replay.DynamoDbAttributeValueSnapshot
import io.github.huherto.awsLambdaStream.faults.replay.DynamoDbReplayRecord
import io.github.huherto.awsLambdaStream.faults.replay.DynamoDbStreamReplayData
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.encodeToJsonElement
import kotlinx.serialization.json.jsonObject
import java.nio.ByteBuffer
import java.util.*

class DynamoDbRecordSnapshotter : RecordSnapshotter {
    override fun supports(record: Any): Boolean {
        return record is DynamodbEvent.DynamodbStreamRecord
    }

    override fun snapshot(record: Any): ReplayRecordSnapshot {
        val dynamodbRecord = record as DynamodbEvent.DynamodbStreamRecord

        val replayRecord = DynamoDbReplayRecord(
            eventID = dynamodbRecord.eventID,
            eventName = dynamodbRecord.eventName,
            eventVersion = dynamodbRecord.eventVersion,
            eventSource = dynamodbRecord.eventSource,
            eventSourceARN = dynamodbRecord.eventSourceARN,
            awsRegion = dynamodbRecord.awsRegion,
            dynamodb = DynamoDbStreamReplayData(
                approximateCreationDateTime = dynamodbRecord.dynamodb.approximateCreationDateTime?.time?.toDouble()?.div(1000.0),
                keys = dynamodbRecord.dynamodb.keys?.mapValues { toSnapshot(it.value) },
                newImage = dynamodbRecord.dynamodb.newImage?.mapValues { toSnapshot(it.value) },
                oldImage = dynamodbRecord.dynamodb.oldImage?.mapValues { toSnapshot(it.value) },
                sequenceNumber = dynamodbRecord.dynamodb.sequenceNumber,
                sizeBytes = dynamodbRecord.dynamodb.sizeBytes,
                streamViewType = dynamodbRecord.dynamodb.streamViewType
            )
        )

        return ReplayRecordSnapshot(
            kind = "dynamodb",
            payload = Json.encodeToJsonElement(replayRecord).jsonObject
        )
    }

    private fun toSnapshot(av: AttributeValue): DynamoDbAttributeValueSnapshot {
        return DynamoDbAttributeValueSnapshot(
            S = av.s,
            N = av.n,
            B = av.b?.let { encodeByteBuffer(it) },
            BOOL = av.getBOOL(),
            NULL = av.getNULL(),
            M = av.m?.mapValues { toSnapshot(it.value) },
            L = av.l?.map { toSnapshot(it) },
            SS = av.getSS(),
            NS = av.getNS(),
            BS = av.getBS()?.map { encodeByteBuffer(it) }
        )
    }

    private fun encodeByteBuffer(bb: ByteBuffer): String {
        val duplicate = bb.duplicate()
        val bytes = ByteArray(duplicate.remaining())
        duplicate.get(bytes)
        return Base64.getEncoder().encodeToString(bytes)
    }
}
