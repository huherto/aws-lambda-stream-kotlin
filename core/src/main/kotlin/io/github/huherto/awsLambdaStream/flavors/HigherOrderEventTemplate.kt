package io.github.huherto.awsLambdaStream.flavors

import io.github.huherto.awsLambdaStream.BaseEvent
import io.github.huherto.awsLambdaStream.Event

// A concrete implementation of Event to create Higher Order Events
data class HigherOrderEventTemplate (
    val baseEvent: Event, // Base event to copyEvent from
    override val id: String? = null,
    override val timestamp: Long? = null,
    override val partitionKey: String? = null,
    override val tags: Map<String, String>? = null,
    override val raw: io.github.huherto.awsLambdaStream.RawRecord? = null,
    @kotlinx.serialization.Contextual
    override val eem: Any? = null,
    override val triggers: List<io.github.huherto.awsLambdaStream.EventReference>? = null
) : BaseEvent() {
    override fun eventType(): String = "Not used"

    override fun toString(): String = "Not used"

    fun createEvent(clazz: kotlin.reflect.KClass<out Event>): Event {
        val instance = io.github.huherto.awsLambdaStream.utils.createFromCommonValues(baseEvent, clazz)
        return applyTemplate(instance)        // Override with template's own values
    }

    fun applyTemplate(
        instance: Event,
    ): Event {
        return instance.copyEvent(
            id = this.id,
            timestamp = this.timestamp,
            partitionKey = this.partitionKey,
            tags = this.tags,
            raw = this.raw,
            eem = this.eem,
            triggers = this.triggers
        )
    }
}