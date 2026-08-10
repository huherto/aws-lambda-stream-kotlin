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

/**
 * Base for kotlinx serializers that delegate to the AWS `LambdaEventSerializers` codecs.
 *
 * The record is embedded as real JSON rather than an escaped string, so a serialized event
 * carries the AWS-canonical record shape and stays readable as a replay fixture.
 */
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

object DynamodbStreamRecordSerializer : AwsRecordSerializer<DynamodbEvent.DynamodbStreamRecord>(
    encode = DynamodbStreamRecordReplayJson::encode,
    decode = DynamodbStreamRecordReplayJson::decode,
)

object KinesisEventRecordSerializer : AwsRecordSerializer<KinesisEvent.KinesisEventRecord>(
    encode = KinesisEventRecordReplayJson::encode,
    decode = KinesisEventRecordReplayJson::decode,
)

object SQSMessageSerializer : AwsRecordSerializer<SQSEvent.SQSMessage>(
    encode = SQSMessageReplayJson::encode,
    decode = SQSMessageReplayJson::decode,
)
