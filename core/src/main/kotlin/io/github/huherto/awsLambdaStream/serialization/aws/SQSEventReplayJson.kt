package io.github.huherto.awsLambdaStream.serialization.aws

import com.amazonaws.services.lambda.runtime.events.SQSEvent
import com.amazonaws.services.lambda.runtime.serialization.events.LambdaEventSerializers
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream

object SQSEventReplayJson {
    private val serializer =
        LambdaEventSerializers.serializerFor(
            SQSEvent::class.java,
            Thread.currentThread().contextClassLoader
        )

    fun encode(event: SQSEvent): String {
        val out = ByteArrayOutputStream()
        serializer.toJson(event, out)
        return out.toString(Charsets.UTF_8)
    }

    fun decode(json: String): SQSEvent {
        return serializer.fromJson(
            ByteArrayInputStream(json.toByteArray())
        )
    }
}
