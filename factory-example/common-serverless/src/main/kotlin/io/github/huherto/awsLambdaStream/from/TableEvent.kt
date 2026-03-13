package io.github.huherto.awsLambdaStream.from

import io.github.huherto.awsLambdaStream.Event
import io.github.huherto.awsLambdaStream.asJson
import kotlinx.serialization.Contextual
import kotlinx.serialization.Serializable
import org.myorg.sut.TrackedUnit

@Serializable
class TableEvent : Event {
    override var id: String? = null
    override var timestamp: Long? = null
    override var partitionKey: String? = null
    override var tags: Map<String, String>? = HashMap<String, String>()

    var type: String? = null

    @Contextual
    override var raw: Any? = null

    @Contextual
    override var eem: Any? = null
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