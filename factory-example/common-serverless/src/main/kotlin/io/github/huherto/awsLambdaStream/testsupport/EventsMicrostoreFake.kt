package io.github.huherto.awsLambdaStream.testsupport

import io.github.huherto.awsLambdaStream.EventsMicrostore
import io.github.huherto.awsLambdaStream.UnitOfWork
import kotlinx.coroutines.flow.Flow

class EventsMicrostoreFake : EventsMicrostore {

    private val events: MutableMap< String, UnitOfWork > = mutableMapOf()

    fun reset() {
        events.clear()
    }

    // What should we return here ?
    fun getEvents(): Map< String, UnitOfWork > = events.toMap()

    override suspend fun save(
        flow: Flow<UnitOfWork>,
        options: EventsMicrostore.SaveOptions
    ) {
        flow.collect { it ->
            it.event?.id?.let { key -> events.put(key, it) }
        }
    }

}