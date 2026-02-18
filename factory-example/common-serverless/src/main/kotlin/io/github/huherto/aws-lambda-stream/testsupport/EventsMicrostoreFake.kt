package io.github.huherto.`aws-lambda-stream`.testsupport

import io.github.huherto.`aws-lambda-stream`.Event
import io.github.huherto.`aws-lambda-stream`.EventsMicrostore
import io.github.huherto.`aws-lambda-stream`.UnitOfWork
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.onEach
import java.util.stream.Stream

class EventsMicrostoreFake<E : Event> : EventsMicrostore<E> {

    private val events: MutableMap< String, UnitOfWork<E> > = mutableMapOf()

    fun reset() {
        events.clear()
    }

    // What should we return here ?
    fun getEvents(): Map< String, UnitOfWork<E> > = events.toMap()

    override suspend fun save(
        flow: Flow<UnitOfWork<E>>,
        options: EventsMicrostore.SaveOptions
    ) {
        flow.collect { it ->
            it.event?.id?.let { key -> events.put(key, it) }
        }
    }

}