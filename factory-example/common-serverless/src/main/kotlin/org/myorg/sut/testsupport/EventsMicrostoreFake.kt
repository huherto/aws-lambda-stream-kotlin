package org.myorg.sut.testsupport

import org.myorg.sut.Event
import org.myorg.sut.EventsMicrostore
import org.myorg.sut.Thing
import org.myorg.sut.UnitOfWork
import java.util.stream.Stream

class EventsMicrostoreFake<E : Event> : EventsMicrostore<E> {

    private val events: MutableList< UnitOfWork<E> > = mutableListOf()

    fun reset() {
        events.clear()
    }

    // What should we return here ?
    fun getEvents(): List< UnitOfWork<E> > = events.toList()

    override fun save(
        stream: Stream<UnitOfWork<E>>,
        options: EventsMicrostore.SaveOptions
    ) {
        stream.forEach { events.add(it) }
    }

}