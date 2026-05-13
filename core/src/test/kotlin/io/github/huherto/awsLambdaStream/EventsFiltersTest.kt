package io.github.huherto.awsLambdaStream

import io.github.huherto.awsLambdaStream.filters.EventFilters
import io.github.huherto.awsLambdaStream.filters.filterEvents
import io.github.huherto.awsLambdaStream.sinks.EventPublisherInMemory
import io.mockk.spyk
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class EventsFiltersTest {

    private val envConfig = spyk(EnvironmentConfig())

    @Test
    fun `test any filter matches all events`() {
        val filter = EventFilters.any()

        assertTrue { filter.matches(MyEventA()) }
        assertTrue { filter.matches(MyEventB()) }
        assertTrue { filter.matches(MyEventC()) }
    }

    @Test
    fun `test regex filter matches event type by pattern`() {
        val filter = EventFilters.regex("MY_EVENT_[AB]")

        assertTrue { filter.matches(MyEventA()) }
        assertTrue { filter.matches(MyEventB()) }
        assertFalse { filter.matches(MyEventC()) }
    }

    @Test
    fun `test onContent filter matches event type by  predicate`() {
        val filter = EventFilters.onContent { event ->
            event.eventType().contains("MY_EVENT_A") }

        assertTrue { filter.matches(MyEventA()) }
        assertFalse { filter.matches(MyEventB()) }
        assertFalse { filter.matches(MyEventC()) }
    }

    @Test
    fun `test anyOf filter matches when any nested filter matches`() {
        val filter = EventFilters.anyOf(
            EventFilters.name("MY_EVENT_A"),
            EventFilters.name("MY_EVENT_B")
        )

        assertTrue { filter.matches(MyEventA()) }
        assertTrue { filter.matches(MyEventB()) }
        assertFalse { filter.matches(MyEventC()) }
    }

    @Test
    fun `test allOf filter matches when all nested filters match`() {
        val filter = EventFilters.allOf(
            EventFilters.regex("MY_EVENT_.*"),
            EventFilters.name("MY_EVENT_A")
        )

        assertTrue { filter.matches(MyEventA()) }
        assertFalse { filter.matches(MyEventB()) }
        assertFalse { filter.matches(MyEventC()) }
    }

    @Test
    fun `test event types filter`() = runTest {

        val faultManager = FaultManager(envConfig = envConfig, eventPublisher = EventPublisherInMemory())

        val  myEventA = MyEventA()
        val  myEventB = MyEventB()
        val  myEventC = MyEventC()

        var foundMyEventA = false
        var foundMyEventB = false
        var foundMyEventC = false

        flow{
            emit(UnitOfWork(event = myEventA ))
            emit(UnitOfWork(event = myEventB ))
            emit(UnitOfWork(event = myEventC ))
        }.filterEvents(faultManager, EventFilters.classes(MyEventA::class, MyEventB::class))
            .onEach {
                when (it.event) {
                    is MyEventA -> foundMyEventA = true
                    is MyEventB -> foundMyEventB = true
                    is MyEventC -> foundMyEventC = true
                    null -> TODO()
                }
            }.collect {  }
        assertTrue { foundMyEventA }
        assertTrue { foundMyEventB }
        assertFalse { foundMyEventC }
    }

}