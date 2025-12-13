package org.myorg.sut.testsupport

import org.myorg.sut.Event
import org.myorg.sut.EventsMicrostore
import org.myorg.sut.UnitOfWork
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