package io.github.huherto.awsLambdaStream.from

import io.github.huherto.awsLambdaStream.BaseEvent
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json.Default.encodeToString

@Serializable
class TableChangeEvent : BaseEvent() {

    var type: String? = null

    override fun eventType(): String {
        return type ?: "table_change"
    }

    override fun toString(): String {
        return encodeToString(this)
    }

    override fun encoded(): String {
        return encodeToString(this)
    }

}