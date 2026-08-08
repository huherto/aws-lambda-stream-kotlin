package io.github.huherto.awsLambdaStream.from

import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.fasterxml.jackson.databind.annotation.JsonSerialize
import io.github.huherto.awsLambdaStream.BaseEvent
import io.github.huherto.awsLambdaStream.serialization.RecordPairAsJsonObjectSerializer
import io.github.huherto.awsLambdaStream.serialization.RecordPairJsonDeserializer
import io.github.huherto.awsLambdaStream.serialization.RecordPairJsonSerializer
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json.Default.encodeToString

@Serializable
data class TableChangeEvent(
    override val id: String? = null,
    override val timestamp: Long? = null,
    override val partitionKey: String? = null,
    override val tags: Map<String, String>? = null,
    @Serializable(with = RecordPairAsJsonObjectSerializer::class)
    @JsonSerialize(using = RecordPairJsonSerializer::class)
    @JsonDeserialize(using = RecordPairJsonDeserializer::class)
    @kotlinx.serialization.Contextual
    override val raw: Any? = null,
    @kotlinx.serialization.Contextual
    override val eem: Any? = null,
    override val triggers: List<io.github.huherto.awsLambdaStream.EventReference>? = null,
    val type: String? = null,
) : BaseEvent() {

    override fun eventType(): String {
        return type ?: "table_change"
    }

    override fun toString(): String {
        return encodeToString(this)
    }
}