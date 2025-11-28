package org.myorg.sut

import java.util.stream.Stream

class EventsMicrostoreFake<T : Thing> : EventsMicrostore<T> {

    private val events: MutableList< UnitOfWork<T> > = mutableListOf()

    fun reset() {
        events.clear()
    }

    // What should we return here ?
    fun getEvents(): List< UnitOfWork<T> > = events.toList()

    override fun save(
        stream: Stream<UnitOfWork<T>>,
        options: EventsMicrostore.SaveOptions
    ) {
        stream.forEach { events.add(it) }
    }

}