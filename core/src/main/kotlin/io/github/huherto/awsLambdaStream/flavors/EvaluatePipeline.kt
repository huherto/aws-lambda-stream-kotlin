package io.github.huherto.awsLambdaStream.flavors

import com.amazonaws.services.lambda.runtime.events.DynamodbEvent
import io.github.huherto.awsLambdaStream.*
import io.github.huherto.awsLambdaStream.filters.EventFilter
import io.github.huherto.awsLambdaStream.filters.filterEvents
import io.github.huherto.awsLambdaStream.from.RecordImage
import io.github.huherto.awsLambdaStream.from.RecordPair
import io.github.huherto.awsLambdaStream.from.TableChangeEvent
import io.github.huherto.awsLambdaStream.sinks.EventPublisher
import io.github.huherto.awsLambdaStream.sinks.EventsMicrostore
import io.github.huherto.awsLambdaStream.utils.createFromCommonValues
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.*
import kotlin.reflect.KClass
import kotlin.reflect.full.primaryConstructor

// A concrete implementation of Event to create Higher Order Events
data class HigherOrderEventTemplate (
    var clazz: KClass<out Event>? = null,
    var baseEvent: Event? = null,
) : BaseEvent() {
    override fun eventType(): String = "Not used"
    override fun encoded(): String = "Not used"

    fun createEvent(clazz:KClass<out Event>): Event {
        return createEventFromTemplate( clazz, this)
    }

    fun createEvent(): Event {
        return createEventFromTemplate( clazz!!, this)
    }
}

internal fun createEventFromTemplate(
    clazz: KClass<out Event>,
    template: HigherOrderEventTemplate
): Event {
    val baseEvent = template.baseEvent
    val instance = when {
        baseEvent!= null -> createFromCommonValues<Event>(baseEvent, clazz)
        else -> clazz.primaryConstructor!!.call() as Event
    }

    // Override with template's own values
    return instance.apply {
        id = template.id
        timestamp = template.timestamp
        partitionKey = template.partitionKey
        tags = template.tags
        raw = template.raw
        eem = template.eem
        triggers = template.triggers
    }
}

sealed interface EmitOption {
    data class Basic(val clazz: Class<out Event>) : EmitOption
    data class Custom(val emit: (UnitOfWork, HigherOrderEventTemplate) -> List<Event>) : EmitOption
}

class EvaluatePipeline (
    id: String,
    val envConfig: EnvironmentConfig,
    val eventPublisher: EventPublisher,
    val eventsMicrostore: EventsMicrostore,
    val onContentType: (UnitOfWork) -> Boolean = { true },
    val eventFilter: EventFilter = EventFilter.Any,
    val correlationKeySuffix: String = "",
    val index: String? = null,
    val bufferCapacity: Int = Channel.BUFFERED,
    val eventCodec: EventCodec,
    val expression: ((UnitOfWork) -> Boolean)? = null,
    val higherOrderEmit: EmitOption? = null,
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

        return uow.copy(
            queryParams = queryParams,
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
        return eventsMicrostore.queryByPk(this)
    }

    internal fun Flow<UnitOfWork>.complex(): Flow<UnitOfWork> {
        return if (expression == null) {
            this.map { uow ->
                uow.copy(
                    triggers = listOfNotNull(uow.event)
                )
            }
        } else {
            this
                .filter { uow -> onCorrelationKeySuffix(uow) }
                .queryCorrelated()
                .filter { uow ->
                    expression.invoke(uow)
                }
        }
    }

    internal fun toHigherOrderEvents(uow: UnitOfWork): List<UnitOfWork> {

        val template = toHigherOrderEventTemplate(uow)

        val instantiatedEvent: Event = if (higherOrderEmit is EmitOption.Basic && template.clazz != null) {
            template.createEvent()
        } else {
            template
        }

        val resultEvents: List<Event> = when (higherOrderEmit) {
            is EmitOption.Basic -> listOf(instantiatedEvent)
            is EmitOption.Custom -> higherOrderEmit.emit(uow, template)
            null -> throw IllegalArgumentException("higherOrderEmit must be a String or a function")
        }

        // Maps results back to a UnitOfWork (replacing the current event with the emitted one)
        return resultEvents.map { emit ->
            uow.copy(event = emit)
        }
    }

    internal fun toHigherOrderEventTemplate(uow: UnitOfWork): HigherOrderEventTemplate {
        val basic = higherOrderEmit is EmitOption.Basic
        val trigger = uow.triggers?.lastOrNull()

        val aggregatedTags = aggregateTags(uow)

        val mappedTriggers = uow.triggers?.map {
            EventReference(it.id, it.eventType(), it.timestamp)
        }

        val template = HigherOrderEventTemplate().apply {
            id = uow.meta?.get("eventId")
            partitionKey = uow.meta?.get("partitionKey")
            clazz = (higherOrderEmit as? EmitOption.Basic)?.clazz?.kotlin
            timestamp = trigger?.timestamp
            tags = aggregatedTags
            this.triggers = mappedTriggers
            baseEvent = if (basic) uow.event else null
            raw = if (basic) uow.event?.raw else null
            eem = if (basic) uow.event?.eem else null
        }

        return template
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
                .filter{ uow -> faulty(uow){ forEvents(uow) } == true }
                .mapNotFaulty{  uow -> normalize(uow) }
                .filterEvents(fm, eventFilter)
                .onEach { uow -> printStartPipeline(uow) }
                .filter { uow -> faulty(uow) { onContentType(uow) } == true }
                .complex()
                .flatMapMerge { uow ->
                    faulty(uow) { toHigherOrderEvents(uow) }?.asFlow() ?: emptyFlow()
                }
                .buffer(bufferCapacity)
                .publish()
                .onEach { uow -> printEndPipeline(uow) }
            return flow
        }
    }
}
