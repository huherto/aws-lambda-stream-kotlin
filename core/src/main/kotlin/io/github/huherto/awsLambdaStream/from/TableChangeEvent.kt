package io.github.huherto.awsLambdaStream.from

import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.fasterxml.jackson.databind.annotation.JsonSerialize
import io.github.huherto.awsLambdaStream.BaseEvent
import io.github.huherto.awsLambdaStream.serialization.RecordPairAsJsonObjectSerializer
import io.github.huherto.awsLambdaStream.serialization.RecordPairJsonDeserializer
import io.github.huherto.awsLambdaStream.serialization.RecordPairJsonSerializer
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json.Default.encodeToString

class TableChangeEvent : BaseEvent() {

    var type: String? = null

    @Serializable(with = RecordPairAsJsonObjectSerializer::class)
    @JsonSerialize(using = RecordPairJsonSerializer::class)
    @JsonDeserialize(using = RecordPairJsonDeserializer::class)
    override var raw: Any? = null

    override fun eventType(): String {
        return type ?: "table_change"
    }

    override fun toString(): String {
        return encodeToString(this)
    }

    @Deprecated(
        message = "Use EventCodec or the configured framework publisher instead.",
    )
    override fun encoded(): String {
        return encodeToString(this)
    }

}