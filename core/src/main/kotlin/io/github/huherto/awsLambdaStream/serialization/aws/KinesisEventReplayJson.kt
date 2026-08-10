package io.github.huherto.awsLambdaStream.serialization.aws

import com.amazonaws.services.lambda.runtime.events.KinesisEvent
import com.amazonaws.services.lambda.runtime.serialization.events.LambdaEventSerializers
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.nio.charset.StandardCharsets

object KinesisEventReplayJson {
    private val serializer =
        LambdaEventSerializers.serializerFor(
            KinesisEvent::class.java,
            Thread.currentThread().contextClassLoader
        )

    fun encode(event: KinesisEvent): String {
        val out = ByteArrayOutputStream()
        serializer.toJson(event, out)
        return out.toString(StandardCharsets.UTF_8)
    }

    fun decode(json: String): KinesisEvent {
        return serializer.fromJson(
            ByteArrayInputStream(json.toByteArray(StandardCharsets.UTF_8))
        )
    }
}