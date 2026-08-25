package io.github.huherto.awsLambdaStream.serialization.aws

import com.amazonaws.services.lambda.runtime.events.SQSEvent
import io.github.huherto.awsLambdaStream.serialization.aws.snapshots.AwsJson
import io.github.huherto.awsLambdaStream.serialization.aws.snapshots.SQSMessageSnapshot
import io.github.huherto.awsLambdaStream.serialization.aws.snapshots.toRecord
import io.github.huherto.awsLambdaStream.serialization.aws.snapshots.toSnapshot

object SQSMessageReplayJson {

    fun encode(message: SQSEvent.SQSMessage): String {
        return AwsJson.encodeToString(message.toSnapshot())
    }

    fun decode(json: String): SQSEvent.SQSMessage {
        return AwsJson.decodeFromString<SQSMessageSnapshot>(json).toRecord()
    }
}
