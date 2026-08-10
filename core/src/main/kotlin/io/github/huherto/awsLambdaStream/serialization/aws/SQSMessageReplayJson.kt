package io.github.huherto.awsLambdaStream.serialization.aws

import com.amazonaws.services.lambda.runtime.events.SQSEvent
import com.amazonaws.services.lambda.runtime.serialization.events.LambdaEventSerializers
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.nio.charset.StandardCharsets

object SQSMessageReplayJson {
    private val serializer =
        LambdaEventSerializers.serializerFor(
            SQSEvent.SQSMessage::class.java,
            Thread.currentThread().contextClassLoader
        )

    fun encode(message: SQSEvent.SQSMessage): String {
        val out = ByteArrayOutputStream()
        serializer.toJson(message, out)
        return out.toString(StandardCharsets.UTF_8)
    }

    fun decode(json: String): SQSEvent.SQSMessage {
        return serializer.fromJson(
            ByteArrayInputStream(json.toByteArray(StandardCharsets.UTF_8))
        )
    }
}
