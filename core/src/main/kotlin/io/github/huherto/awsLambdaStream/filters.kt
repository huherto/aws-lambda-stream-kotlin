package io.github.huherto.awsLambdaStream

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.filter
import kotlin.reflect.KClass

fun Flow<UnitOfWork>.filterEvents(
    faultManager: FaultManager,
    filter: EventFilter
): Flow<UnitOfWork> = filter { uow ->
    with(faultManager) {
        faulty(uow) {
            val event = uow.event
            event != null && filter.matches(event)
        } == true
    }
}

// TODO: Remove when not needed any longer.
//
fun Flow<UnitOfWork>.filterEventTypes(
    faultManager: FaultManager,
    vararg klassList: KClass<out Event>
): Flow<UnitOfWork> =
    filterEvents(faultManager, EventFilter.ByClass(klassList.toList()))

fun Flow<UnitOfWork>.filterEventName(
    faultManager: FaultManager,
    name: String
): Flow<UnitOfWork> =
    filterEvents(faultManager, EventFilter.ByName(name))

fun Flow<UnitOfWork>.filterEventNameRegex(
    faultManager: FaultManager,
    regex: Regex
): Flow<UnitOfWork> =
    filterEvents(faultManager, EventFilter.ByRegex(regex))

sealed interface EventFilter {
    fun matches(event: Event): Boolean

    data object Any : EventFilter {
        override fun matches(event: Event): Boolean = true
    }

    data class ByClass(
        val classes: List<KClass<out Event>>
    ) : EventFilter {
        override fun matches(event: Event): Boolean =
            classes.any { it.isInstance(event) }
    }

    data class ByName(
        val name: String
    ) : EventFilter {
        override fun matches(event: Event): Boolean =
            event.eventType() == name
    }

    data class ByRegex(
        val regex: Regex
    ) : EventFilter {
        override fun matches(event: Event): Boolean =
            regex.matches(event.eventType())
    }

    data class AnyOf(
        val filters: List<EventFilter>
    ) : EventFilter {
        override fun matches(event: Event): Boolean =
            filters.any { it.matches(event) }
    }

    data class AllOf(
        val filters: List<EventFilter>
    ) : EventFilter {
        override fun matches(event: Event): Boolean =
            filters.all { it.matches(event) }
    }
}

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