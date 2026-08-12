package io.github.huherto.awsLambdaStream.serialization.aws

import com.amazonaws.services.lambda.runtime.events.KinesisEvent
import com.amazonaws.services.lambda.runtime.serialization.events.LambdaEventSerializers
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream

object KinesisEventRecordReplayJson {
    private val serializer =
        LambdaEventSerializers.serializerFor(
            KinesisEvent.KinesisEventRecord::class.java,
            Thread.currentThread().contextClassLoader
        )

    fun encode(record: KinesisEvent.KinesisEventRecord): String {
        val out = ByteArrayOutputStream()
        serializer.toJson(record, out)
        return out.toString(Charsets.UTF_8)
    }

    fun decode(json: String): KinesisEvent.KinesisEventRecord {
        return serializer.fromJson(
            ByteArrayInputStream(json.toByteArray())
        )
    }
}
