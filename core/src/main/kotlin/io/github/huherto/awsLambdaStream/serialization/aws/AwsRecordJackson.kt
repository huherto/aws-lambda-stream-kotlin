package io.github.huherto.awsLambdaStream.serialization.aws

import com.amazonaws.services.lambda.runtime.events.DynamodbEvent
import com.amazonaws.services.lambda.runtime.events.KinesisEvent
import com.amazonaws.services.lambda.runtime.events.SQSEvent
import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.*

/**
 * Jackson counterparts of [AwsRecordSerializer]. They route through the same
 * `LambdaEventSerializers` codecs so Jackson and kotlinx emit identical JSON for a record.
 */
abstract class AwsRecordJacksonSerializer<T>(
    private val encode: (T) -> String,
) : JsonSerializer<T>() {
    override fun serialize(value: T?, gen: JsonGenerator, serializers: SerializerProvider) {
        if (value == null) {
            gen.writeNull()
            return
        }
        gen.writeRawValue(encode(value))
    }
}

abstract class AwsRecordJacksonDeserializer<T>(
    private val decode: (String) -> T,
) : JsonDeserializer<T>() {
    override fun deserialize(parser: JsonParser, context: DeserializationContext): T? {
        val node = parser.codec.readTree<JsonNode>(parser) ?: return null
        if (node.isNull) return null
        return decode(node.toString())
    }
}

class DynamodbStreamRecordJacksonSerializer :
    AwsRecordJacksonSerializer<DynamodbEvent.DynamodbStreamRecord>(DynamodbStreamRecordReplayJson::encode)

class DynamodbStreamRecordJacksonDeserializer :
    AwsRecordJacksonDeserializer<DynamodbEvent.DynamodbStreamRecord>(DynamodbStreamRecordReplayJson::decode)

class KinesisEventRecordJacksonSerializer :
    AwsRecordJacksonSerializer<KinesisEvent.KinesisEventRecord>(KinesisEventRecordReplayJson::encode)

class KinesisEventRecordJacksonDeserializer :
    AwsRecordJacksonDeserializer<KinesisEvent.KinesisEventRecord>(KinesisEventRecordReplayJson::decode)

class SQSMessageJacksonSerializer :
    AwsRecordJacksonSerializer<SQSEvent.SQSMessage>(SQSMessageReplayJson::encode)

class SQSMessageJacksonDeserializer :
    AwsRecordJacksonDeserializer<SQSEvent.SQSMessage>(SQSMessageReplayJson::decode)
