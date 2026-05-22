package io.github.huherto.awsLambdaStream.flavors

import com.amazonaws.services.lambda.runtime.events.DynamodbEvent
import io.github.huherto.awsLambdaStream.*
import io.github.huherto.awsLambdaStream.filters.EventFilter
import io.github.huherto.awsLambdaStream.filters.filterEvents
import io.github.huherto.awsLambdaStream.from.RecordImage
import io.github.huherto.awsLambdaStream.from.RecordPair
import io.github.huherto.awsLambdaStream.sinks.EventsMicrostore
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.onEach

/**
 * Discriminator used for records persisted by [CorrelatePipeline].
 */
const val CORREL = "CORREL"

/**
 * Pipeline flavor that correlates previously collected events and persists correlation records.
 *
 * A `CorrelatePipeline` consumes DynamoDB stream records created by the collection stage, normalizes
 * the stored event payload back into an [Event], optionally filters it, derives a correlation key, and
 * saves a correlation entry into an [EventsMicrostore].
 *
 * The saved records use:
 * - `pk`: the derived correlation key plus [correlationKeySuffix]
 * - `sk`: the event id
 * - `discriminator`: [CORREL]
 * - `suffix`: [correlationKeySuffix]
 *
 * These records can later be queried by evaluation pipelines to find events that share the same
 * correlation key.
 *
 * @param id Unique identifier for this pipeline.
 * @param onContentType Predicate used to accept or reject a [UnitOfWork] after event filtering.
 * @param eventFilter Event-level filter applied after the source record has been normalized.
 * @param correlationKey Function used to derive the key under which the event is correlated.
 * Must be configured before the pipeline processes records.
 * @param correlationKeySuffix Optional suffix appended to the correlation key. Use this when the
 * same logical key needs to support multiple independent rule sets.
 * @param envConfig Environment-backed configuration used for AWS region lookup.
 * @param eventsMicrostore Sink responsible for persisting correlation records.
 * @param eventCodec Optional event deserializer. When `null`, events are parsed as [JsonEvent].
 * @param expire Whether the microstore record should be written as expirable.
 */
class CorrelatePipeline(
    id: String,
    val onContentType: (UnitOfWork) -> Boolean = { true },
    val eventFilter: EventFilter = EventFilter.Any,
    val correlationKey: ((UnitOfWork) -> String)? = null,
    val correlationKeySuffix: String = "",
    val envConfig: EnvironmentConfig,
    var eventsMicrostore: EventsMicrostore,
    val eventCodec: EventCodec? = null,
    val expire: Boolean = false,
) : Pipeline(id) {

    /**
     * Returns `true` when the unit of work represents a collected event record that can be correlated.
     *
     * The pipeline only accepts DynamoDB `INSERT` records whose sort key is `"EVENT"` and whose raw
     * payload contains a [RecordPair].
     */
    internal fun forCollectedEvents(uow: UnitOfWork) : Boolean {
        return when(uow.record) {
            is DynamodbEvent.DynamodbStreamRecord -> {
                uow.record.eventName == "INSERT"
                        && uow.record.dynamodb.keys["sk"]?.s == "EVENT"
                        && uow.event?.raw is RecordPair
            }
            else -> false
        }
    }

    /**
     * Deserializes a persisted event payload.
     *
     * If [unmarshall] is configured, it is used as the custom decoder. Otherwise, the payload is parsed
     * as a [JsonEvent].
     *
     * @throws Exception when the payload cannot be parsed.
     */
    internal fun defaultUnmarshall(eventAsString: String) : Event {
        if (eventCodec != null) return eventCodec.decode(eventAsString)

        val jsonEvent: JsonEvent = try {
            JsonEvent(eventAsString)
        } catch (e: Exception) {
            logger.error {"Failed to parse event: $eventAsString, $e" }
            throw e
        }
        return jsonEvent
    }

    /**
     * Converts a collected DynamoDB stream unit of work into a normalized event unit of work.
     *
     * This extracts the new record image, deserializes the stored event payload, and attaches metadata
     * used when writing the correlation record.
     */
    internal fun normalize(uow: UnitOfWork): UnitOfWork {
        val raw = uow.event?.raw as? RecordPair
        val rawNew = raw?.new ?: RecordImage(mapOf())
        val eventAsString = rawNew.getEvent()?: "{}"
        val eventAsObject = defaultUnmarshall(eventAsString)
        val record = uow.record as? DynamodbEvent.DynamodbStreamRecord

        return uow.copy(
            meta = mapOf(
                "sequenceNumber" to record?.dynamodb?.sequenceNumber,
                "ttl" to "" + rawNew.getTtl().toString(),
                "data" to "" + rawNew.getData(),
            ),
            event = eventAsObject
        )
    }

    /**
     * Derives and attaches the correlation key for a unit of work.
     *
     * The configured [correlationKeySuffix] is appended to the key so separate rule sets can reuse the
     * same base key without sharing correlation records.
     *
     * @throws IllegalArgumentException when [correlationKey] is not configured.
     */
    private fun addCorrelationKey(uow: UnitOfWork) : UnitOfWork {
        require(correlationKey != null) { "correlationKey must be set" }

        // use a suffix when you need the same key for different sets of rules
        val key = correlationKey(uow) + correlationKeySuffix
        return uow.copy(key = key)
    }

    /**
     * Builds save options for each valid correlated event and delegates persistence to [eventsMicrostore].
     *
     * Unit-of-work items without a key, event, or event id pass through unchanged. The microstore itself
     * is responsible for handling save failures.
     */
    internal fun Flow<UnitOfWork>.save(): Flow<UnitOfWork> {
        val flow = this.map { uow ->
            val key = uow.key ?: return@map uow
            val event: Event = uow.event ?: return@map uow
            val eventId = event.id ?: return@map uow
            val saveOptions = EventsMicrostore.SaveOptions(
                pk = key,
                sk = eventId,
                discriminator = CORREL,
                timeStamp = uow.event.timestamp,
                awsRegion = envConfig.awsRegion(),
                sequenceNumber = uow.meta?.get("sequenceNumber"),
                ttl = uow.meta?.get("ttl")?.toLongOrNull(),
                expire = expire,
                suffix = correlationKeySuffix,
                pipelineId = id,
            )
            uow.copy(saveOptions = saveOptions)
        }
        // Save already has a fault manager.
        return eventsMicrostore.save(flow)
    }

    /**
     * Connects this pipeline to an upstream [Flow].
     *
     * Processing order:
     * 1. Keep only supported collected event records.
     * 2. Normalize stored event data.
     * 3. Apply [eventFilter].
     * 4. Log pipeline start.
     * 5. Apply [onContentType].
     * 6. Derive and attach the correlation key.
     * 7. Persist the correlation record through [save].
     * 8. Log pipeline completion.
     *
     * @param fm Fault manager used by filtering and mapping stages.
     * @param fromFlow Upstream flow of units of work.
     * @return A flow that emits the processed units of work.
     */
    override fun connect(fm: FaultManager, fromFlow: Flow<UnitOfWork>) : Flow<UnitOfWork> {
        logger.info { "CorrelatePipeline.connect: id=$id" }
        with(fm) {
            val flow = fromFlow
                .filterNotFaulty{ uow ->  forCollectedEvents(uow) }
                .mapNotFaulty{  uow -> normalize(uow) }
                .filterEvents(this, eventFilter)
                .onEach { uow -> printStartPipeline(uow) }
                .filterNotFaulty { uow -> onContentType(uow) }
                .mapNotFaulty { uow -> addCorrelationKey(uow)}
                .save()
                .onEach { uow -> printEndPipeline(uow) }
            return flow
        }
    }
}