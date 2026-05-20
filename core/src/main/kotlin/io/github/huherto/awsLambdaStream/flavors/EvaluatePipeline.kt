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
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.*

/**
 * Configures how an [EvaluatePipeline] emits higher-order events after an evaluation succeeds.
 */
sealed interface EmitOption {
    /**
     * Emits a single event by instantiating [clazz] from a [HigherOrderEventTemplate].
     */
    data class Basic(val clazz: Class<out Event>) : EmitOption

    /**
     * Emits one or more custom events from the current [UnitOfWork] and generated
     * [HigherOrderEventTemplate].
     */
    data class Custom(val emit: (UnitOfWork, HigherOrderEventTemplate) -> List<Event>) : EmitOption
}

/**
 * Pipeline flavor that evaluates collected and correlated events and publishes higher-order events.
 *
 * An `EvaluatePipeline` consumes DynamoDB stream records written by the collection/correlation
 * stages, normalizes the stored event payload, optionally queries correlated events, evaluates an
 * expression, converts successful matches into higher-order events, and publishes the emitted
 * events through [eventPublisher].
 *
 * Processing supports two modes:
 * - Simple evaluation, when [expression] is `null`: the normalized event becomes the only trigger.
 * - Complex evaluation, when [expression] is provided: matching correlation records are queried
 *   from [eventsMicrostore] before the expression is evaluated.
 *
 * @param id Unique identifier for this pipeline.
 * @param envConfig Environment-backed configuration available to the pipeline.
 * @param eventPublisher Sink responsible for publishing emitted higher-order events.
 * @param eventsMicrostore Store used to query collected or correlated events.
 * @param onContentType Predicate used to accept or reject a [UnitOfWork] after event filtering.
 * @param eventFilter Event-level filter applied after the source record has been normalized.
 * @param correlationKeySuffix Suffix used to select the correlation set evaluated by this pipeline.
 * @param index Optional microstore index used when querying correlated records.
 * @param bufferCapacity Capacity used by the flow buffer before publishing emitted events.
 * @param eventCodec Codec used to deserialize stored event payloads.
 * @param expression Optional predicate evaluated against a normalized/correlated [UnitOfWork].
 * When `null`, every filtered event is treated as a successful match.
 * @param higherOrderEmit Strategy used to create emitted higher-order events.
 */
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

    /**
     * Returns `true` when the unit of work represents a DynamoDB stream record this pipeline can
     * evaluate.
     *
     * The pipeline accepts newly inserted collected events and correlation records.
     */
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

    /**
     * Deserializes a stored event payload using [eventCodec].
     */
    internal fun defaultUnmarshall(eventAsString: String) : Event {
        return eventCodec.decode(eventAsString)
    }

    /**
     * Converts a table-change unit of work into a normalized event unit of work.
     *
     * This extracts the persisted event payload, decodes it, builds query parameters for later
     * correlation lookups, and attaches metadata used when emitting higher-order events.
     */
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

    /**
     * Checks whether the unit of work belongs to this pipeline's configured correlation suffix.
     */
    internal fun onCorrelationKeySuffix(uow: UnitOfWork): Boolean {
        val uowSuffix = uow.meta?.get("suffix") ?: ""
        return correlationKeySuffix == uowSuffix
    }

    /**
     * Queries events correlated with each upstream unit of work.
     *
     * The microstore query operation is responsible for its own fault handling.
     */
    internal fun Flow<UnitOfWork>.queryCorrelated() : Flow<UnitOfWork> {
        // queryByPK already has a fault manager.
        return eventsMicrostore.queryByPk(this)
    }

    /**
     * Applies the evaluation stage.
     *
     * If [expression] is not configured, each unit of work is marked with its current event as the
     * sole trigger. If [expression] is configured, correlated records are queried first and only
     * units of work for which the expression returns `true` continue downstream.
     */
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

    /**
     * Creates emitted higher-order event units of work from a successfully evaluated unit of work.
     *
     * The emitted event list is produced according to [higherOrderEmit], then each event is wrapped
     * back into a copy of the original unit of work.
     *
     * @throws IllegalArgumentException when [higherOrderEmit] is not configured.
     */
    internal fun toHigherOrderEvents(uow: UnitOfWork): List<UnitOfWork> {

        val template = toHigherOrderEventTemplate(uow)

        val resultEvents: List<Event> = when (higherOrderEmit) {
            is EmitOption.Basic -> listOf(template.createEvent(higherOrderEmit.clazz.kotlin))
            is EmitOption.Custom -> higherOrderEmit.emit(uow, template)
            null -> throw IllegalArgumentException("higherOrderEmit must be a String or a function")
        }

        // Maps results back to a UnitOfWork (replacing the current event with the emitted one)
        return resultEvents.map { emit ->
            uow.copy(event = emit)
        }
    }

    /**
     * Builds the [HigherOrderEventTemplate] used by [toHigherOrderEvents].
     *
     * The template carries identity, partition key, timestamp, trigger references, and aggregated
     * tags from the evaluated unit of work.
     */
    internal fun toHigherOrderEventTemplate(uow: UnitOfWork): HigherOrderEventTemplate {
        val basic = higherOrderEmit is EmitOption.Basic
        val trigger = uow.triggers?.lastOrNull()
        val baseEvent = uow.event ?: throw IllegalArgumentException("Event is null")

        val aggregatedTags = aggregateTags(uow)

        val mappedTriggers = uow.triggers?.map {
            EventReference(it.id, it.eventType(), it.timestamp)
        }

        val template = HigherOrderEventTemplate(baseEvent = baseEvent).apply {
            id = uow.meta?.get("eventId")
            partitionKey = uow.meta?.get("partitionKey")
            timestamp = trigger?.timestamp
            tags = aggregatedTags
            this.triggers = mappedTriggers
            raw = if (basic) baseEvent.raw else null
            eem = if (basic) baseEvent.eem else null
        }

        return template
    }

    /**
     * Merges trigger tags into a single tag map for the emitted higher-order event.
     *
     * Framework-level `region` and `source` tags are removed from the aggregate.
     */
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

    /**
     * Publishes emitted events through [eventPublisher].
     */
    internal fun Flow<UnitOfWork>.publish() : Flow<UnitOfWork> {
        return eventPublisher.publish(this)
    }

    /**
     * Connects this pipeline to an upstream [Flow].
     *
     * Processing order:
     * 1. Keep only supported DynamoDB stream records.
     * 2. Normalize stored event data.
     * 3. Apply [eventFilter].
     * 4. Log pipeline start.
     * 5. Apply [onContentType].
     * 6. Evaluate simple or complex rules through [complex].
     * 7. Convert successful matches to higher-order events.
     * 8. Buffer emitted units of work.
     * 9. Publish emitted events.
     * 10. Log pipeline completion.
     *
     * @param fm Fault manager used by filtering, mapping, and emission stages.
     * @param fromFlow Upstream flow of units of work.
     * @return A flow that emits the processed units of work.
     */
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
}
