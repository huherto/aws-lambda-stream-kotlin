package io.github.huherto.awsLambdaStream.filters

import io.github.huherto.awsLambdaStream.Event
import kotlin.reflect.KClass

object EventFilters {

    fun any(): EventFilter = EventFilter.Any

    fun classes(vararg classes: KClass<out Event>): EventFilter =
        EventFilter.ByClass(classes.toList())

    fun name(value: String): EventFilter =
        EventFilter.ByName(value)

    fun regex(pattern: String): EventFilter =
        EventFilter.ByRegex(Regex(pattern))

    fun anyOf(vararg filters: EventFilter): EventFilter =
        EventFilter.AnyOf(filters.toList())

    fun allOf(vararg filters: EventFilter): EventFilter =
        EventFilter.AllOf(filters.toList())
}