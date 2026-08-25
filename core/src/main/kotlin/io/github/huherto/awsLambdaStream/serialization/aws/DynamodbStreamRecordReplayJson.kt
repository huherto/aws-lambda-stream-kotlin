package io.github.huherto.awsLambdaStream.serialization.aws

import com.amazonaws.services.lambda.runtime.events.DynamodbEvent
import io.github.huherto.awsLambdaStream.serialization.aws.snapshots.AwsJson
import io.github.huherto.awsLambdaStream.serialization.aws.snapshots.DynamodbStreamRecordSnapshot
import io.github.huherto.awsLambdaStream.serialization.aws.snapshots.toRecord
import io.github.huherto.awsLambdaStream.serialization.aws.snapshots.toSnapshot

object DynamodbStreamRecordReplayJson {

    fun encode(record: DynamodbEvent.DynamodbStreamRecord): String {
        return AwsJson.encodeToString(record.toSnapshot())
    }

    fun decode(json: String): DynamodbEvent.DynamodbStreamRecord {
        return AwsJson.decodeFromString<DynamodbStreamRecordSnapshot>(json).toRecord()
    }
}
