package io.github.huherto.awsLambdaStream.sinks

import io.github.huherto.awsLambdaStream.UnitOfWork
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.onEach

class EventsMicrostoreInMemory : EventsMicrostore {

    private val uowMap: MutableMap< String, UnitOfWork> = mutableMapOf()

    fun reset() {
        uowMap.clear()
    }

    fun saveUowMap(): Map< String, UnitOfWork> = uowMap.toMap()

    override fun save(flow: Flow<UnitOfWork>): Flow<UnitOfWork> {
        return flow.onEach {
            uowMap.put(it.event?.id?:"unknown id", it)
        }
    }

}