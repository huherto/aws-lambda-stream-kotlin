package io.github.huherto.awsLambdaStream.filters

import io.github.huherto.awsLambdaStream.Event
import kotlin.reflect.KClass

object EventFilters {

    @JvmStatic
    fun any(): EventFilter = EventFilter.Any

    @JvmStatic
    fun classes(vararg classes: KClass<out Event>): EventFilter =
        EventFilter.ByClass(classes.toList())

    @JvmStatic
    fun classes(vararg classes: Class<out Event>): EventFilter =
        EventFilter.ByClass(classes.map { it.kotlin })

    @JvmStatic
    fun name(value: String): EventFilter =
        EventFilter.ByName(value)

    @JvmStatic
    fun regex(pattern: String): EventFilter =
        EventFilter.ByRegex(Regex(pattern))

    @JvmStatic
    fun onContent(predicate: (Event) -> Boolean): EventFilter =
        EventFilter.OnContent(predicate)

    @JvmStatic
    fun anyOf(vararg filters: EventFilter): EventFilter =
        EventFilter.AnyOf(filters.toList())

    @JvmStatic
    fun allOf(vararg filters: EventFilter): EventFilter =
        EventFilter.AllOf(filters.toList())
}