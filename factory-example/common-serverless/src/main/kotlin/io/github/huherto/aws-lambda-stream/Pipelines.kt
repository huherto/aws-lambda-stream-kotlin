package io.github.huherto.`aws-lambda-stream`

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.flow.map
import kotlin.reflect.KClass

class Pipelines {
}

class CollectPipeline(var eventsMicrostore: EventsMicrostore) {

    private var onContentType: (UnitOfWork) -> Boolean = { true }

    private var onEventClass: List<KClass<Event>> = listOf(Event::class);

    private var correlationKey: (UnitOfWork) -> String? = {
            uom -> uom.event?.partitionKey
    }

    suspend fun collect(fromFlow: Flow<UnitOfWork>) {

        val flow = fromFlow
            .filter { uom -> uom.event != null }
            .filterEventTypes(*onEventClass.toTypedArray())
            .filter {
                faulty(it) {
                    onContentType(it)
                }
            }
            .map {
                faulty(it) {
                    // TODO: Should use it.copy(). uom should be an immutable class.
                    it.key = correlationKey(it)
                    it
                }
            }

        eventsMicrostore.save(flow, EventsMicrostore.SaveOptions(90))
    }

}

