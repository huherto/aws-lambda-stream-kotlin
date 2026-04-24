package io.github.huherto.awsLambdaStream.flavors

import io.github.huherto.awsLambdaStream.BaseEvent
import io.github.huherto.awsLambdaStream.Event
import io.github.huherto.awsLambdaStream.utils.createFromCommonValues
import kotlin.reflect.KClass
import kotlin.reflect.full.primaryConstructor

// A concrete implementation of Event to create Higher Order Events
data class HigherOrderEventTemplate (
    var clazz: KClass<out Event>? = null, // Class of the event to instantiate
    var baseEvent: Event? = null, // Base event to copy from
) : BaseEvent() {
    override fun eventType(): String = "Not used"
    override fun encoded(): String = "Not used"

    fun createEvent(clazz: KClass<out Event>): Event {
        return createEventFromTemplate( clazz, this)
    }

    fun createEvent(): Event {
        val clazz = clazz ?: throw IllegalArgumentException("clazz must be set")
        return createEventFromTemplate( clazz, this)
    }

    internal fun createEventFromTemplate(
        clazz: KClass<out Event>,
        template: HigherOrderEventTemplate
    ): Event {
        val baseEvent = template.baseEvent
        val instance = when {
            baseEvent!= null -> createFromCommonValues<Event>(baseEvent, clazz)
            else -> clazz.primaryConstructor!!.call() as Event
        }

        // Override with template's own values
        return instance.apply {
            id = template.id
            timestamp = template.timestamp
            partitionKey = template.partitionKey
            tags = template.tags
            raw = template.raw
            eem = template.eem
            triggers = template.triggers
        }
    }
}