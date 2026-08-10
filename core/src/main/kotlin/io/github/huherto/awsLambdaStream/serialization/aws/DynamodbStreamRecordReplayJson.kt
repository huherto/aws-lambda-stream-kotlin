package io.github.huherto.awsLambdaStream.serialization.aws

import com.amazonaws.services.lambda.runtime.events.DynamodbEvent
import com.amazonaws.services.lambda.runtime.serialization.events.LambdaEventSerializers
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.nio.charset.StandardCharsets

object DynamodbStreamRecordReplayJson {
    private val serializer =
        LambdaEventSerializers.serializerFor(
            DynamodbEvent.DynamodbStreamRecord::class.java,
            Thread.currentThread().contextClassLoader
        )

    fun encode(record: DynamodbEvent.DynamodbStreamRecord): String {
        val out = ByteArrayOutputStream()
        serializer.toJson(record, out)
        return out.toString(StandardCharsets.UTF_8)
    }

    fun decode(json: String): DynamodbEvent.DynamodbStreamRecord {
        return serializer.fromJson(
            ByteArrayInputStream(json.toByteArray(StandardCharsets.UTF_8))
        )
    }
}
