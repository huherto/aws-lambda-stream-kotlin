package io.github.huherto.`aws-lambda-stream`.testsupport

import io.github.huherto.`aws-lambda-stream`.Event
import io.github.huherto.`aws-lambda-stream`.EventsMicrostore
import io.github.huherto.`aws-lambda-stream`.UnitOfWork
import java.util.stream.Stream

class EventsMicrostoreFake<E : Event> : EventsMicrostore<E> {

    private val events: MutableMap< String, UnitOfWork<E> > = mutableMapOf()

    fun reset() {
        events.clear()
    }

    // What should we return here ?
    fun getEvents(): Map< String, UnitOfWork<E> > = events.toMap()

    override fun save(
        stream: Stream<UnitOfWork<E>>,
        options: EventsMicrostore.SaveOptions
    ) {
        stream.forEach {
            it?.event?.id?.let { key -> events.put(key, it) }
        }
    }

}