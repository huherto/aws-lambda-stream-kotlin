package io.github.huherto.awsLambdaStream

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.onEach
import mu.KotlinLogging
import kotlin.reflect.KClass

abstract class Pipeline {

    protected val logger = KotlinLogging.logger {  }

    fun printStartPipeline(uom: UnitOfWork) {
        logger.debug { "start type: ${uom.event?.type}, eid: ${uom.event?.id}" }
    }

    fun printEndPipeline(uom: UnitOfWork) {
        val redacted = trimAndRedacted(uom)
        logger.debug { "end type: ${uom.event?.type}, eid: ${uom.event?.id}, uow: $redacted" }
    }

    fun trimAndRedacted(uom: UnitOfWork) {
        uom
    }

}

class CollectPipeline(var eventsMicrostore: EventsMicrostore) : Pipeline() {

    private var onContentType: (UnitOfWork) -> Boolean = { true }

    private var onEventClass: List<KClass<Event>> = listOf(Event::class);

    private var correlationKey: (UnitOfWork) -> String? = {
            uom -> uom.event?.partitionKey
    }

    suspend fun collect(fromFlow: Flow<UnitOfWork>) {

        val flow = fromFlow
            .filter { uom -> uom.event != null }
            .filterEventTypes(*onEventClass.toTypedArray())
            .onEach {  printStartPipeline(it) }
            .filter {
                faulty(it) {
                    onContentType(it)
                }
            }
            .map {
                faulty(it) {
                    it.copy(key = correlationKey(it))
                }
            }
            .onEach {  printEndPipeline(it) }

        eventsMicrostore.save(flow, EventsMicrostore.SaveOptions(90))
    }

}



