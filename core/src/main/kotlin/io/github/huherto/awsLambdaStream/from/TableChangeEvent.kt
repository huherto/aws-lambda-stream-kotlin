package io.github.huherto.awsLambdaStream.from

import io.github.huherto.awsLambdaStream.BaseEvent
import io.github.huherto.awsLambdaStream.asJson
import kotlinx.serialization.Serializable

@Serializable
class TableChangeEvent : BaseEvent() {

    var type: String? = null

    override fun eventType(): String {
        return type ?: "unknown"
    }

    override fun toString(): String {
        return this.asJson()
    }

    override fun encoded(): String {
        return this.asJson()
    }

}