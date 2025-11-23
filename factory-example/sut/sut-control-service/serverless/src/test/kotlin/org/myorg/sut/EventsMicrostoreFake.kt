package org.myorg.sut

import com.amazonaws.services.lambda.runtime.events.KinesisEvent
import java.util.stream.Stream

class EventsMicrostoreFake<T : Thing> : EventsMicrostore<T> {

    private val events: MutableList<KinesisEvent> = mutableListOf()

    fun reset() {
        events.clear()
    }

    fun getEvents(): List<KinesisEvent> = events.toList()

    override fun save(
        stream: Stream<UnitOfWork<T>>,
        options: EventsMicrostore.SaveOptions
    ) {
        TODO("Not yet implemented")
    }

}