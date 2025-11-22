package org.myorg.sut

import com.amazonaws.services.lambda.runtime.events.KinesisEvent

class EventsMicrostoreFake<T : Thing> : EventsMicrostore<T> {

    private val events: MutableList<KinesisEvent> = mutableListOf()

    override fun save(event: KinesisEvent) {
        events.add(event)
    }

    fun reset() {
        events.clear()
    }

    fun getEvents(): List<KinesisEvent> = events.toList()

}