package io.github.huherto.awsLambdaStream.serialization.aws

import com.amazonaws.services.lambda.runtime.events.KinesisEvent
import io.github.huherto.awsLambdaStream.serialization.aws.snapshots.AwsJson
import io.github.huherto.awsLambdaStream.serialization.aws.snapshots.KinesisEventRecordSnapshot
import io.github.huherto.awsLambdaStream.serialization.aws.snapshots.toRecord
import io.github.huherto.awsLambdaStream.serialization.aws.snapshots.toSnapshot

object KinesisEventRecordReplayJson {

    fun encode(record: KinesisEvent.KinesisEventRecord): String {
        return AwsJson.encodeToString(record.toSnapshot())
    }

    fun decode(json: String): KinesisEvent.KinesisEventRecord {
        return AwsJson.decodeFromString<KinesisEventRecordSnapshot>(json).toRecord()
    }
}
