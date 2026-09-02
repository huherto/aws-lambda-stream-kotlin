package io.github.huherto.awsLambdaStream.flavors

import com.amazonaws.services.lambda.runtime.events.DynamodbEvent
import io.github.huherto.awsLambdaStream.*
import io.github.huherto.awsLambdaStream.extensions.withQueryParams
import io.github.huherto.awsLambdaStream.faults.FaultManager
import io.github.huherto.awsLambdaStream.filters.EventFilter
import io.github.huherto.awsLambdaStream.filters.filterEvents
import io.github.huherto.awsLambdaStream.from.RecordImage
import io.github.huherto.awsLambdaStream.from.RecordPair
import io.github.huherto.awsLambdaStream.from.TableChangeEvent
import io.github.huherto.awsLambdaStream.sinks.EventPublisher
import io.github.huherto.awsLambdaStream.sinks.EventsMicrostore
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.*


/** Pipeline flavor that evaluates collected and correlated events and publishes higher-order events. */
class EvaluatePipeline(
    id: String,
    val eventPublisher: EventPublisher,
    val eventsMicrostore: EventsMicrostore,
    val onContentType: (UnitOfWork) -> Boolean,
    val eventFilter: EventFilter,
    val correlationKeySuffix: String,
    val index: String?,
    val bufferCapacity: Int,
    val eventCodec: EventCodec,
    val expression: ((UnitOfWork) -> Boolean)?,
    val emit: ((UnitOfWork) -> (List<Event>))?,
) : Pipeline(id) {

    internal fun forEvents(uow: UnitOfWork) : Boolean {
        return when(uow.record) {
            is DynamodbEvent.DynamodbStreamRecord -> {
                (uow.record.eventName == "INSERT"
                        && uow.record.dynamodb?.keys["sk"]?.s == "EVENT")
                        || uow.record.dynamodb?.newImage?.get("discriminator")?.s == "CORREL"
            }
            else -> false
        }
    }

    internal fun defaultUnmarshall(eventAsString: String) : Event {
        return eventCodec.decode(eventAsString)
    }

    internal fun normalize(uow: UnitOfWork): UnitOfWork {

        val tableChangeEvent = uow.event as? TableChangeEvent ?: return uow
        val raw = tableChangeEvent.raw as? RecordPair ?: return uow

        val rawNew = raw.new ?: RecordImage(mapOf())
        val eventAsString = rawNew.getEvent()?: "{}"
        val eventAsObject = defaultUnmarshall(eventAsString)
        val correlation = rawNew.getDiscriminator() == "CORREL"
        val pk = rawNew.getPk()
        val data = rawNew.getData()
        val suffix = rawNew.getSuffix()
        val queryParams = EventsMicrostore.QueryParams(
            pk = pk,
            correlation =  correlation,
            data = data,
            index = index,
        )

        val correlationKey = if (correlation) pk else data
        val partitionKey = correlationKey?.replace(".${suffix}", "")

        return uow.withQueryParams(queryParams).copy(
            event = eventAsObject,
            meta = mapOf(
                "eventId" to "${tableChangeEvent.id}.${id}",
                "partitionKey" to partitionKey,
            )
        )
    }

    internal fun onCorrelationKeySuffix(uow: UnitOfWork): Boolean {
        val uowSuffix = uow.meta?.get("suffix") ?: ""
        return correlationKeySuffix == uowSuffix
    }

    internal fun Flow<UnitOfWork>.queryCorrelated() : Flow<UnitOfWork> {
        // queryByPK already has a fault manager.
        return eventsMicrostore.queryByPk(this)
    }

    internal fun Flow<UnitOfWork>.complex(fm : FaultManager): Flow<UnitOfWork> {
        return if (expression == null) {
            this.map { uow ->
                uow.copy(
                    triggers = listOfNotNull(uow.event)
                )
            }
        } else {
            this
                .filter { uow -> fm.faulty(uow) { onCorrelationKeySuffix(uow) } == true }
                .queryCorrelated()
                .mapNotNull { uow ->
                    val result = fm.faulty(uow) { expression(uow) }
                    if (result == true) {
                        uow.copy(triggers = listOfNotNull(uow.event))
                    } else {
                        null
                    }
                }

        }
    }

    internal fun toHigherOrderEvents(uow: UnitOfWork): List<UnitOfWork> {
        val emit = this.emit ?: return emptyList()
        val triggeringEvent = uow.event ?: return emptyList()

        val eventId = uow.meta?.get("eventId")
        val partitionKey = uow.meta?.get("partitionKey")
        val trigger = uow.triggers?.lastOrNull()
        val aggregatedTags = aggregateTags(uow)
        val mappedTriggers = uow.triggers?.map {
            EventReference(it.id, it.eventType(), it.timestamp)
        }

        val resultEvents: List<Event> = emit(uow)

        return resultEvents.map { e ->
            val event = e.copyEvent(
                id = eventId,
                timestamp = trigger?.timestamp,
                partitionKey = partitionKey,
                tags = aggregatedTags,
                triggers = mappedTriggers,
                raw = e.raw ?: triggeringEvent.raw,
                eem = e.eem ?: triggeringEvent.eem
            )
            uow.copy(event = event)
        }
    }


    private fun aggregateTags(uow: UnitOfWork): MutableMap<String, String>? {
        // reduce + merge + omit(['region', 'source'])
        val aggregatedTags = uow.triggers
            ?.mapNotNull { it.tags }
            ?.fold(mutableMapOf<String, String>()) { acc, currentTags ->
                acc.apply { putAll(currentTags) }
            }?.apply {
                remove("region")
                remove("source")
            }
        return aggregatedTags
    }

    internal fun Flow<UnitOfWork>.publish() : Flow<UnitOfWork> {
        return eventPublisher.publish(this)
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    override fun connect(fm: FaultManager, fromFlow: Flow<UnitOfWork>) : Flow<UnitOfWork> {
        logger.info { "Evaluate.connect: id=$id" }
        with(fm) {
            val flow = fromFlow
                .filterNotFaulty{ uow -> forEvents(uow) }
                .mapNotFaulty{  uow -> normalize(uow) }
                .filterEvents(fm, eventFilter)
                .onEach { uow -> printStartPipeline(uow) }
                .filterNotFaulty { uow -> onContentType(uow) }
                .complex(fm)
                .flatMapMerge { uow ->
                    faulty(uow) { toHigherOrderEvents(uow) }?.asFlow() ?: emptyFlow()
                }
                .buffer(bufferCapacity)
                .publish()
                .onEach { uow -> printEndPipeline(uow) }
            return flow
        }
    }

    companion object {
        @JvmStatic
        fun builder() = Builder()
    }

    class Builder : PipelineBuilder<EvaluatePipeline, Builder>() {
        private var eventPublisher: EventPublisher? = null
        private var eventsMicrostore: EventsMicrostore? = null
        private var onContentType: (UnitOfWork) -> Boolean = { true }
        private var eventFilter: EventFilter = EventFilter.Any
        private var correlationKeySuffix: String = ""
        private var index: String? = null
        private var bufferCapacity: Int = Channel.BUFFERED
        private var eventCodec: EventCodec? = null
        private var expression: ((UnitOfWork) -> Boolean)? = null
        private var emit: ((UnitOfWork) -> List<Event>)? = null

        fun eventPublisher(eventPublisher: EventPublisher) = apply { this.eventPublisher = eventPublisher }
        fun eventsMicrostore(eventsMicrostore: EventsMicrostore) = apply { this.eventsMicrostore = eventsMicrostore }
        fun onContentType(onContentType: (UnitOfWork) -> Boolean) = apply { this.onContentType = onContentType }
        fun onContentType(onContentType: java.util.function.Predicate<UnitOfWork>) = apply { this.onContentType = { uow -> onContentType.test(uow) } }
        fun eventFilter(eventFilter: EventFilter) = apply { this.eventFilter = eventFilter }
        fun correlationKeySuffix(correlationKeySuffix: String) = apply { this.correlationKeySuffix = correlationKeySuffix }
        fun index(index: String?) = apply { this.index = index }
        fun bufferCapacity(bufferCapacity: Int) = apply { this.bufferCapacity = bufferCapacity }
        fun eventCodec(eventCodec: EventCodec) = apply { this.eventCodec = eventCodec }
        fun expression(expression: (UnitOfWork) -> Boolean) = apply { this.expression = expression }
        fun expressionJava(expression: java.util.function.Predicate<UnitOfWork>) = apply { this.expression = { uow -> expression.test(uow) } }
        fun emit(emit: (UnitOfWork) -> List<Event>) = apply { this.emit = emit }
        fun emitJava(emit: java.util.function.Function<UnitOfWork, List<Event>>) = apply { this.emit = { uow -> emit.apply(uow) } }

        override fun build(): EvaluatePipeline {
            return EvaluatePipeline(
                id = id ?: throw IllegalArgumentException("id is required"),
                eventPublisher = eventPublisher ?: throw IllegalArgumentException("eventPublisher is required"),
                eventsMicrostore = eventsMicrostore ?: throw IllegalArgumentException("eventsMicrostore is required"),
                onContentType = onContentType,
                eventFilter = eventFilter,
                correlationKeySuffix = correlationKeySuffix,
                index = index,
                bufferCapacity = bufferCapacity,
                eventCodec = eventCodec ?: throw IllegalArgumentException("eventCodec is required"),
                expression = expression,
                emit = emit
            )
        }
    }
}
