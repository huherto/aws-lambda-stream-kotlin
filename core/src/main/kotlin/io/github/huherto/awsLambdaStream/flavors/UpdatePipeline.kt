package io.github.huherto.awsLambdaStream.flavors

import aws.sdk.kotlin.services.dynamodb.model.BatchGetItemRequest
import aws.sdk.kotlin.services.dynamodb.model.QueryRequest
import aws.sdk.kotlin.services.dynamodb.model.UpdateItemRequest
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent
import io.github.huherto.awsLambdaStream.*
import io.github.huherto.awsLambdaStream.connectors.DynamoDbConnector
import io.github.huherto.awsLambdaStream.filters.EventFilter
import io.github.huherto.awsLambdaStream.filters.filterEvents
import io.github.huherto.awsLambdaStream.from.RecordImage
import io.github.huherto.awsLambdaStream.from.RecordPair
import io.github.huherto.awsLambdaStream.queries.DynamoDbQuery
import io.github.huherto.awsLambdaStream.sinks.DynamoDbSink
import io.github.huherto.awsLambdaStream.utils.CompactRule
import io.github.huherto.awsLambdaStream.utils.compact
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.flatMapConcat
import kotlinx.coroutines.flow.onEach

/**
 * Pipeline flavor that reacts to incoming events, queries related DynamoDB records, loads those
 * records, builds update requests, and applies the updates.
 */
class UpdatePipeline(
    id: String,
    private val envConfig: EnvironmentConfig,
    private val dynamoDbConnector: DynamoDbConnector? = null,
    private val dynamoDbSink: DynamoDbSink = DynamoDbSink(envConfig, dynamoDbConnector),
    private val dynamoDbQuery: DynamoDbQuery = DynamoDbQuery(envConfig, dynamoDbConnector),
    private val eventCodec: EventCodec,
    private val eventFilter: EventFilter = EventFilter.Any,
    private val onContentType: (UnitOfWork) -> Boolean = { true },
    private val compactRule: CompactRule? = null,
    private val toQueryRequest: ((UnitOfWork) -> QueryRequest?)? = null,
    private val toGetRequest: ((UnitOfWork) -> BatchGetItemRequest?)? = null,
    private val toUpdateRequest: suspend (UnitOfWork) -> UpdateItemRequest?,
) : Pipeline(id) {

    /**
     * Returns `true` when the unit of work represents a collected event record that should be
     * normalized before normal update processing.
     */
    internal fun forCollectedEvents(uow: UnitOfWork): Boolean {
        return when (uow.record) {
            is DynamodbEvent.DynamodbStreamRecord -> {
                uow.record.eventName == "INSERT" &&
                        uow.record.dynamodb.keys["sk"]?.s == "EVENT" &&
                        uow.event?.raw is RecordPair
            }

            else -> false
        }
    }

    /**
     * Deserializes the event payload stored in a collected DynamoDB record.
     */
    internal fun decodeEvent(eventAsString: String): Event {
        return eventCodec.decode(eventAsString)
    }

    /**
     * Normalizes collected event records back into event units of work.
     *
     * Non-collected events pass through unchanged:
     *
     * `collected INSERT EVENT ? normalize(uow) : uow`
     */
    internal fun normalizeIfCollectedEvent(uow: UnitOfWork): UnitOfWork {
        if (!forCollectedEvents(uow)) {
            return uow
        }

        val raw = uow.event?.raw as? RecordPair
        val rawNew = raw?.new ?: RecordImage(mapOf())
        val eventAsString = rawNew.getEvent() ?: "{}"
        val eventAsObject = decodeEvent(eventAsString)
        val record = uow.record as? DynamodbEvent.DynamodbStreamRecord

        if (eventAsObject.id == null) {
            logger.warn { "Event id is null: $eventAsString" }
        }

        return uow.copy(
            meta = mapOf(
                "sequenceNumber" to record?.dynamodb?.sequenceNumber,
                "ttl" to rawNew.getTtl().toString(),
                "data" to rawNew.getData(),
            ),
            event = eventAsObject,
        )
    }

    /**
     * Adds the DynamoDB query request to the unit of work.
     */
    internal fun toQuery(uow: UnitOfWork): UnitOfWork {
        return uow.copy(
            queryRequest = toQueryRequest?.invoke(uow),
        )
    }

    /**
     * Splits a unit of work with a DynamoDB query response into one unit of work per returned item.
     *
     * The current [UnitOfWork] shape does not include a dedicated `queryResponseItem` field, so each
     * split item is represented as a shallow copy carrying a synthetic [batch] with the source item.
     * This preserves the pipeline split behavior while allowing downstream rule functions to inspect
     * the original query response from the copied unit of work if needed.
     */
    internal fun splitQueryResponse(uow: UnitOfWork): List<UnitOfWork> {
        val items = uow.queryResponse?.items.orEmpty()

        if (items.isEmpty()) {
            return listOf(uow)
        }

        return items.map {
            uow.copy()
        }
    }

    /**
     * Adds the DynamoDB batch-get request to the unit of work.
     */
    internal fun toGetRequest(uow: UnitOfWork): UnitOfWork {
        return uow.copy(
            batchGetRequest = toGetRequest?.invoke(uow),
        )
    }

    /**
     * Adds the DynamoDB update request to the unit of work.
     */
    internal suspend fun toUpdateRequest(uow: UnitOfWork): UnitOfWork {
        return uow.copy(
            updateRequest = toUpdateRequest.invoke(uow),
        )
    }

    /**
     * Executes DynamoDB update requests through the configured sink.
     */
    internal fun Flow<UnitOfWork>.updateDynamoDB(fm: FaultManager): Flow<UnitOfWork> {
        return dynamoDbSink.update(fm, this)
    }

    internal fun Flow<UnitOfWork>.queryAllDynamoDB(fm: FaultManager) : Flow<UnitOfWork> {
        return dynamoDbQuery.queryAllDynamoDB(fm, this)
    }

    internal fun Flow<UnitOfWork>.batchGetDynamoDB(fm: FaultManager) : Flow<UnitOfWork> {
        return dynamoDbQuery.batchGetDynamoDB(fm, this)
    }

    /**
     * Connects this update pipeline to an upstream [Flow].
     *
     * 1. Normalize collected event records.
     * 2. Apply event filter.
     * 3. Log start.
     * 4. Apply content filter.
     * 5. Compact.
     * 6. Build query request.
     * 7. Query DynamoDB.
     * 8. Split query response.
     * 9. Build batch-get request.
     * 10. Batch-get DynamoDB.
     * 11. Build update request.
     * 12. Update DynamoDB.
     * 13. Log end.
     */
    override fun connect(fm: FaultManager, fromFlow: Flow<UnitOfWork>): Flow<UnitOfWork> {
        logger.info { "UpdatePipeline.connect: id=$id" }

        with(fm) {
            return fromFlow
                .mapNotFaulty { uow -> normalizeIfCollectedEvent(uow) }
                .filterEvents(fm, eventFilter)
                .onEach { uow -> printStartPipeline(uow) }
                .filterNotFaulty { uow -> onContentType(uow) }
                .compact(compactRule)
                .mapNotFaulty { uow -> toQuery(uow) }
                .queryAllDynamoDB(fm)
                .flatMapConcat { uow -> splitQueryResponse(uow).asFlow() }
                .mapNotFaulty { uow -> toGetRequest(uow) }
                .batchGetDynamoDB(fm)
                .mapNotFaulty { uow -> toUpdateRequest(uow) }
                .updateDynamoDB(fm)
                .onEach { uow -> printEndPipeline(uow) }
        }
    }
}