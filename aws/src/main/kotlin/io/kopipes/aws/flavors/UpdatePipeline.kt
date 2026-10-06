package io.kopipes.aws.flavors

import aws.sdk.kotlin.services.dynamodb.model.BatchGetItemRequest
import aws.sdk.kotlin.services.dynamodb.model.QueryRequest
import aws.sdk.kotlin.services.dynamodb.model.UpdateItemRequest
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent
import io.kopipes.aws.connectors.DynamoDbConnector
import io.kopipes.aws.extensions.queryResponse
import io.kopipes.aws.extensions.withBatchGetRequest
import io.kopipes.aws.extensions.withQueryRequest
import io.kopipes.aws.extensions.withUpdateRequest
import io.kopipes.aws.from.RecordPair
import io.kopipes.aws.queries.DynamoDbQuery
import io.kopipes.aws.sinks.DynamoDbSink
import io.kopipes.core.*
import io.kopipes.core.faults.FaultManager
import io.kopipes.core.filters.EventFilter
import io.kopipes.core.filters.filterEvents
import io.kopipes.core.flavors.Pipeline
import io.kopipes.core.utils.CompactRule
import io.kopipes.core.utils.compact
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.flatMapConcat
import kotlinx.coroutines.flow.onEach

/** Pipeline flavor that reacts to incoming events, queries related DynamoDB records, loads those records, builds update requests, and applies the updates. */
class UpdatePipeline(
    id: String,
    private val dynamoDbConnectorOptions: DynamoDbConnector.Options,
    private val eventCodec: EventCodec,
    private val eventFilter: EventFilter,
    private val onContentType: (UnitOfWork) -> Boolean,
    private val compactRule: CompactRule?,
    private val toQueryRequest: ((UnitOfWork) -> QueryRequest?)?,
    private val toGetRequest: ((UnitOfWork) -> BatchGetItemRequest?)?,
    private val toUpdateRequest: suspend (UnitOfWork) -> UpdateItemRequest?,
) : Pipeline(id) {

    val dynamoDbSink by lazy { DynamoDbSink(DynamoDbConnector(dynamoDbConnectorOptions)) }
    val dynamoDbQuery by lazy { DynamoDbQuery(dynamoDbConnectorOptions) }

    internal fun forCollectedEvents(uow: UnitOfWork): Boolean {
        val record = uow.record
        return when (record) {
            is DynamodbEvent.DynamodbStreamRecord -> {
                record.eventName == "INSERT" &&
                        record.dynamodb.keys["sk"]?.s == "EVENT" &&
                        (uow.event is TableChangeEvent || uow.event?.raw is RecordPair)
            }

            else -> false
        }
    }

    internal fun decodeEvent(eventAsString: String): Event {
        return eventCodec.decode(eventAsString)
    }

    internal fun normalizeIfCollectedEvent(uow: UnitOfWork): UnitOfWork {
        if (!forCollectedEvents(uow)) {
            return uow
        }

        val tableChangeEvent = uow.event as? TableChangeEvent
        val eventAsString = tableChangeEvent?.getEvent() ?: (uow.event?.raw as? RecordPair)?.new?.getEvent() ?: "{}"
        val eventAsObject = decodeEvent(eventAsString)
        val record = uow.record as? DynamodbEvent.DynamodbStreamRecord
        val ttl = tableChangeEvent?.getTtl() ?: (uow.event?.raw as? RecordPair)?.new?.getTtl()
        val data = tableChangeEvent?.getData() ?: (uow.event?.raw as? RecordPair)?.new?.getData()

        if (eventAsObject.id == null) {
            logger.warn { "Event id is null: $eventAsString" }
        }

        return uow.copy(
            meta = mapOf(
                "sequenceNumber" to record?.dynamodb?.sequenceNumber,
                "ttl" to ttl.toString(),
                "data" to data,
            ),
            event = eventAsObject,
        )
    }

    internal fun toQuery(uow: UnitOfWork): UnitOfWork {
        return uow.withQueryRequest(toQueryRequest?.invoke(uow))
    }

    internal fun splitQueryResponse(uow: UnitOfWork): List<UnitOfWork> {
        val items = uow.queryResponse?.items.orEmpty()

        if (items.isEmpty()) {
            return listOf(uow)
        }

        return items.map {
            uow.copy()
        }
    }

    internal fun toGetRequest(uow: UnitOfWork): UnitOfWork {
        return uow.withBatchGetRequest(toGetRequest?.invoke(uow))
    }

    internal suspend fun toUpdateRequest(uow: UnitOfWork): UnitOfWork {
        return uow.withUpdateRequest(toUpdateRequest.invoke(uow))
    }

    internal fun Flow<UnitOfWork>.updateDynamoDB(fm: FaultManager): Flow<UnitOfWork> {
        return dynamoDbSink.update(fm, this)
    }

    internal fun Flow<UnitOfWork>.queryAllDynamoDB(fm: FaultManager) : Flow<UnitOfWork> {
        return dynamoDbQuery.queryAllDynamoDB(fm, this)
    }

    internal fun Flow<UnitOfWork>.batchGetDynamoDB(fm: FaultManager) : Flow<UnitOfWork> {
        return dynamoDbQuery.batchGetDynamoDB(fm, this)
    }

    @OptIn(ExperimentalCoroutinesApi::class)
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

    companion object {
        @JvmStatic
        fun builder() = Builder()
    }

    class Builder : PipelineBuilder<UpdatePipeline, Builder>() {
        private var dynamoDbConnectorOptions = DynamoDbConnector.Options()
        private var eventCodec: EventCodec? = null
        private var eventFilter: EventFilter = EventFilter.Any
        private var onContentType: (UnitOfWork) -> Boolean = { true }
        private var compactRule: CompactRule? = null
        private var toQueryRequest: ((UnitOfWork) -> QueryRequest?)? = null
        private var toGetRequest: ((UnitOfWork) -> BatchGetItemRequest?)? = null
        private var toUpdateRequest: (suspend (UnitOfWork) -> UpdateItemRequest?)? = null

        fun dynamoDbConnectorOptions(options: DynamoDbConnector.Options) = apply { this.dynamoDbConnectorOptions = options }
        fun eventCodec(eventCodec: EventCodec) = apply { this.eventCodec = eventCodec }
        fun eventFilter(eventFilter: EventFilter) = apply { this.eventFilter = eventFilter }
        fun onContentType(onContentType: (UnitOfWork) -> Boolean) = apply { this.onContentType = onContentType }
        fun onContentType(onContentType: java.util.function.Predicate<UnitOfWork>) = apply { this.onContentType = { uow -> onContentType.test(uow) } }
        fun compactRule(compactRule: CompactRule) = apply { this.compactRule = compactRule }
        fun toQueryRequest(toQueryRequest: (UnitOfWork) -> QueryRequest?) = apply { this.toQueryRequest = toQueryRequest }
        fun toQueryRequest(toQueryRequest: java.util.function.Function<UnitOfWork, QueryRequest?>) = apply { this.toQueryRequest = { uow -> toQueryRequest.apply(uow) } }
        fun toGetRequest(toGetRequest: (UnitOfWork) -> BatchGetItemRequest?) = apply { this.toGetRequest = toGetRequest }
        fun toGetRequest(toGetRequest: java.util.function.Function<UnitOfWork, BatchGetItemRequest?>) = apply { this.toGetRequest = { uow -> toGetRequest.apply(uow) } }
        fun toUpdateRequest(toUpdateRequest: suspend (UnitOfWork) -> UpdateItemRequest?) = apply { this.toUpdateRequest = toUpdateRequest }
        fun toUpdateRequest(toUpdateRequest: java.util.function.Function<UnitOfWork, UpdateItemRequest?>) = apply { this.toUpdateRequest = { uow -> toUpdateRequest.apply(uow) } }

        override fun build(): UpdatePipeline {
            return UpdatePipeline(
                id = id ?: throw IllegalArgumentException("id is required"),
                dynamoDbConnectorOptions = dynamoDbConnectorOptions,
                eventCodec = eventCodec ?: throw IllegalArgumentException("eventCodec is required"),
                eventFilter = eventFilter,
                onContentType = onContentType,
                compactRule = compactRule,
                toQueryRequest = toQueryRequest,
                toGetRequest = toGetRequest,
                toUpdateRequest = toUpdateRequest ?: throw IllegalArgumentException("toUpdateRequest is required")
            )
        }
    }
}