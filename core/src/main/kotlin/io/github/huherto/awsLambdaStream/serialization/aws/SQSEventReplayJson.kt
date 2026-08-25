package io.github.huherto.awsLambdaStream.serialization.aws

import com.amazonaws.services.lambda.runtime.events.SQSEvent
import io.github.huherto.awsLambdaStream.serialization.aws.snapshots.AwsJson
import io.github.huherto.awsLambdaStream.serialization.aws.snapshots.SQSEventSnapshot
import io.github.huherto.awsLambdaStream.serialization.aws.snapshots.toRecord
import io.github.huherto.awsLambdaStream.serialization.aws.snapshots.toSnapshot

object SQSEventReplayJson {

    fun encode(event: SQSEvent): String {
        return AwsJson.encodeToString(event.toSnapshot())
    }

    fun decode(json: String): SQSEvent {
        return AwsJson.decodeFromString<SQSEventSnapshot>(json).toRecord()
    }
}
