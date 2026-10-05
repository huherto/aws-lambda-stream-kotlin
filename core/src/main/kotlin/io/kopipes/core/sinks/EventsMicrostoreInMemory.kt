package io.kopipes.core.sinks

import io.kopipes.core.UnitOfWork
import io.kopipes.core.faults.FaultManager
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import java.util.concurrent.ConcurrentHashMap

class EventsMicrostoreInMemory(
    private val faultManager: FaultManager? = null
) : EventsMicrostore {
    private val store = ConcurrentHashMap<String, MutableList<UnitOfWork>>()

    override fun save(flow: Flow<UnitOfWork>): Flow<UnitOfWork> {
        return flow.map { uow ->
            val options = uow.saveOptions
            val pk = options?.pk ?: uow.key ?: uow.event?.id ?: "unknown"
            store.computeIfAbsent(pk) { mutableListOf() }.add(uow)
            uow
        }
    }

    override fun queryByPk(flow: Flow<UnitOfWork>): Flow<UnitOfWork> {
        return flow.map { uow ->
            val params = uow.queryParams
            val pk = params?.pk ?: uow.key ?: uow.event?.id
            val matchedUows = if (pk != null) store[pk] ?: emptyList() else emptyList()
            val correlatedEvents = matchedUows.mapNotNull { it.event }
            uow.copy(correlated = correlatedEvents)
        }
    }

    fun getStored(): Map<String, List<UnitOfWork>> = store.toMap()

    fun saveUowMap(): Map<String, UnitOfWork> = getStored().mapValues { it.value.last() }

    fun clear() {
        store.clear()
    }
}
