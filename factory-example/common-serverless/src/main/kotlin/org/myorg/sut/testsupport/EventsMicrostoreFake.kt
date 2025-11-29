package org.myorg.sut.testsupport

import org.myorg.sut.EventsMicrostore
import org.myorg.sut.Thing
import org.myorg.sut.UnitOfWork
import java.util.stream.Stream

class EventsMicrostoreFake<T : Thing> : EventsMicrostore<T> {

    private val events: MutableList<UnitOfWork<T>> = mutableListOf()

    fun reset() {
        events.clear()
    }

    // What should we return here ?
    fun getEvents(): List<UnitOfWork<T>> = events.toList()

    override fun save(
        stream: Stream<UnitOfWork<T>>,
        options: EventsMicrostore.SaveOptions
    ) {
        stream.forEach { events.add(it) }
    }

}