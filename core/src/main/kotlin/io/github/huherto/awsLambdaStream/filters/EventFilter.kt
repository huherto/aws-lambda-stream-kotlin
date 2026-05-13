package io.github.huherto.awsLambdaStream.filters

import io.github.huherto.awsLambdaStream.Event
import kotlin.reflect.KClass

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

    data class OnContent(
        val predicate: (Event) -> Boolean
    ) : EventFilter {
        override fun matches(event: Event): Boolean =
            predicate(event)
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