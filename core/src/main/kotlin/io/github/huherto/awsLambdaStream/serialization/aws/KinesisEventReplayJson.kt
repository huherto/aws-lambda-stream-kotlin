package io.github.huherto.awsLambdaStream.serialization.aws

import com.amazonaws.services.lambda.runtime.events.KinesisEvent
import io.github.huherto.awsLambdaStream.serialization.aws.snapshots.AwsJson
import io.github.huherto.awsLambdaStream.serialization.aws.snapshots.KinesisEventSnapshot
import io.github.huherto.awsLambdaStream.serialization.aws.snapshots.toRecord
import io.github.huherto.awsLambdaStream.serialization.aws.snapshots.toSnapshot

object KinesisEventReplayJson {

    fun encode(event: KinesisEvent): String {
        return AwsJson.encodeToString(event.toSnapshot())
    }

    fun decode(json: String): KinesisEvent {
        return AwsJson.decodeFromString<KinesisEventSnapshot>(json).toRecord()
    }
}