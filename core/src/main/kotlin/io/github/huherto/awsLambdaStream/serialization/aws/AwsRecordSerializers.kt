package io.github.huherto.awsLambdaStream.serialization.aws

import com.amazonaws.services.lambda.runtime.events.DynamodbEvent
import com.amazonaws.services.lambda.runtime.events.KinesisEvent
import com.amazonaws.services.lambda.runtime.events.SQSEvent
import kotlinx.serialization.KSerializer
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonElement

/** Base for kotlinx serializers that delegate to AWS codecs. */
abstract class AwsRecordSerializer<T>(
    private val encode: (T) -> String,
    private val decode: (String) -> T,
) : KSerializer<T> {

    private val delegate = JsonElement.serializer()

    override val descriptor: SerialDescriptor = delegate.descriptor

    override fun serialize(encoder: Encoder, value: T) {
        encoder.encodeSerializableValue(delegate, Json.parseToJsonElement(encode(value)))
    }

    override fun deserialize(decoder: Decoder): T {
        return decode(decoder.decodeSerializableValue(delegate).toString())
    }
}

/** Serializer for [DynamodbEvent.DynamodbStreamRecord]. */
object DynamodbStreamRecordSerializer : AwsRecordSerializer<DynamodbEvent.DynamodbStreamRecord>(
    encode = DynamodbStreamRecordReplayJson::encode,
    decode = DynamodbStreamRecordReplayJson::decode,
)

/** Serializer for [KinesisEvent.KinesisEventRecord]. */
object KinesisEventRecordSerializer : AwsRecordSerializer<KinesisEvent.KinesisEventRecord>(
    encode = KinesisEventRecordReplayJson::encode,
    decode = KinesisEventRecordReplayJson::decode,
)

/** Serializer for [SQSEvent.SQSMessage]. */
object SQSMessageSerializer : AwsRecordSerializer<SQSEvent.SQSMessage>(
    encode = SQSMessageReplayJson::encode,
    decode = SQSMessageReplayJson::decode,
)
