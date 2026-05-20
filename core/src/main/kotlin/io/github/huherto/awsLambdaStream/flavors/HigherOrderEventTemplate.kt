package io.github.huherto.awsLambdaStream.flavors

import io.github.huherto.awsLambdaStream.BaseEvent
import io.github.huherto.awsLambdaStream.Event
import io.github.huherto.awsLambdaStream.utils.createFromCommonValues
import kotlin.reflect.KClass

// A concrete implementation of Event to create Higher Order Events
data class HigherOrderEventTemplate (
    var clazz: KClass<out Event>? = null, // Class of the event to instantiate
    var baseEvent: Event // Base event to copy from
) : BaseEvent() {
    override fun eventType(): String = "Not used"
    override fun encoded(): String = "Not used"

    fun createEvent(clazz: KClass<out Event>): Event {
        val instance = createFromCommonValues(baseEvent, clazz)
        return applyTemplate(instance)        // Override with template's own values
    }

    fun createEvent(): Event {
        val clazz = clazz ?: throw IllegalArgumentException("clazz must be set")
        val instance = createFromCommonValues(baseEvent, clazz)
        return applyTemplate(instance)        // Override with template's own values
    }

    fun applyTemplate(
        instance: Event,
    ): Event {
        instance.id = this.id
        instance.timestamp = this.timestamp
        instance.partitionKey = this.partitionKey
        instance.tags = this.tags
        instance.raw = this.raw
        instance.eem = this.eem
        instance.triggers = this.triggers
        return instance
    }
}