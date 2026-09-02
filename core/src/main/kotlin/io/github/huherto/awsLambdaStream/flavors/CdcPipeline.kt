package io.github.huherto.awsLambdaStream.flavors

import aws.sdk.kotlin.services.dynamodb.model.BatchGetItemRequest
import aws.sdk.kotlin.services.dynamodb.model.QueryRequest
import io.github.huherto.awsLambdaStream.Event
import io.github.huherto.awsLambdaStream.PipelineBuilder
import io.github.huherto.awsLambdaStream.UnitOfWork
import io.github.huherto.awsLambdaStream.connectors.DynamoDbConnector
import io.github.huherto.awsLambdaStream.extensions.withBatchGetRequest
import io.github.huherto.awsLambdaStream.extensions.withQueryRequest
import io.github.huherto.awsLambdaStream.faults.FaultManager
import io.github.huherto.awsLambdaStream.filters.EventFilter
import io.github.huherto.awsLambdaStream.filters.filterEvents
import io.github.huherto.awsLambdaStream.filters.outLatched
import io.github.huherto.awsLambdaStream.queries.DynamoDbQuery
import io.github.huherto.awsLambdaStream.queries.QueryRule
import io.github.huherto.awsLambdaStream.sinks.EventPublisher
import io.github.huherto.awsLambdaStream.utils.CompactRule
import io.github.huherto.awsLambdaStream.utils.compact
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.buffer
import kotlinx.coroutines.flow.onEach

/** Pipeline flavor for change-data-capture style processing. */
class CdcPipeline(
    id: String,
    private val dynamoDbConnectorOptions: DynamoDbConnector.Options,
    private val eventPublisher: EventPublisher,
    private val eventFilter: EventFilter,
    private val onContentType: (UnitOfWork) -> Boolean,
    private val compactRule: CompactRule?,
    private val queryRule: QueryRule?,
    private val toQueryRequest: (suspend (UnitOfWork) -> QueryRequest?)?,
    private val toBatchGetRequest: (suspend (UnitOfWork) -> BatchGetItemRequest?)?,
    private val toEvent: (suspend (UnitOfWork) -> Event?)?,
    private val encryptEvent: (suspend (UnitOfWork) -> UnitOfWork)?,
    private val parallel: Int,
) : Pipeline(id) {


    val dynamoDbQuery by lazy { DynamoDbQuery(dynamoDbConnectorOptions) }

    internal suspend fun addQueryRequest(uow: UnitOfWork): UnitOfWork {
        val queryRequest = when {
            toQueryRequest != null -> toQueryRequest.invoke(uow)
            queryRule != null -> dynamoDbQuery.toPkQueryRequest(uow, queryRule)
            else -> null
        }

        return uow.withQueryRequest(queryRequest)
    }

    internal suspend fun addBatchGetRequest(uow: UnitOfWork): UnitOfWork {
        return uow.withBatchGetRequest(toBatchGetRequest?.invoke(uow))
    }

    internal suspend fun addEvent(uow: UnitOfWork): UnitOfWork {
        val mapper = toEvent ?: return uow
        return uow.copy(event = mapper(uow))
    }

    internal suspend fun encrypt(uow: UnitOfWork): UnitOfWork {
        return encryptEvent?.invoke(uow) ?: uow
    }

    internal fun Flow<UnitOfWork>.publish(): Flow<UnitOfWork> {
        return eventPublisher.publish(this)
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    override fun connect(
        fm: FaultManager,
        fromFlow: Flow<UnitOfWork>,
    ): Flow<UnitOfWork> {
        logger.info { "CdcPipeline.connect: id=$id" }

        val usesDynamoDb = queryRule != null || toQueryRequest != null || toBatchGetRequest != null

        with(fm) {
            val filteredFlow = fromFlow
                .filterNotFaulty { uow -> outLatched(uow) }
                .filterEvents(fm, eventFilter)
                .onEach { uow -> printStartPipeline(uow) }
                .filterNotFaulty { uow -> onContentType(uow) }
                .compact(compactRule)

            val enrichedFlow = if (usesDynamoDb)  {
                filteredFlow
                    .mapNotFaulty { uow -> addQueryRequest(uow) }
                    .let{ dynamoDbQuery.queryAllDynamoDB(fm, it) }
                    .mapNotFaulty { uow -> addBatchGetRequest(uow) }
                    .let { dynamoDbQuery.batchGetDynamoDB(fm, it) }
            }
            else filteredFlow

            return enrichedFlow
                .mapNotFaulty { uow -> addEvent(uow) }
                .buffer(parallel)
                .mapNotFaulty { uow -> encrypt(uow) }
                .publish()
                .onEach { uow -> printEndPipeline(uow) }
        }
    }

    companion object {
        @JvmStatic
        fun builder() = Builder()
    }

    class Builder : PipelineBuilder<CdcPipeline, Builder>() {
        private var dynamoDbConnectorOptions = DynamoDbConnector.Options()
        private var eventPublisher: EventPublisher? = null
        private var eventFilter: EventFilter = EventFilter.Any
        private var onContentType: (UnitOfWork) -> Boolean = { true }
        private var compactRule: CompactRule? = null
        private var queryRule: QueryRule? = null
        private var toQueryRequest: (suspend (UnitOfWork) -> QueryRequest?)? = null
        private var toBatchGetRequest: (suspend (UnitOfWork) -> BatchGetItemRequest?)? = null
        private var toEvent: (suspend (UnitOfWork) -> Event?)? = null
        private var encryptEvent: (suspend (UnitOfWork) -> UnitOfWork)? = null
        private var parallel: Int = System.getenv("PARALLEL")?.toIntOrNull() ?: 4

        fun dynamoDbConnectorOptions(options: DynamoDbConnector.Options) = apply { this.dynamoDbConnectorOptions = options }
        fun eventPublisher(eventPublisher: EventPublisher) = apply { this.eventPublisher = eventPublisher }
        fun eventFilter(eventFilter: EventFilter) = apply { this.eventFilter = eventFilter }
        fun onContentType(onContentType: (UnitOfWork) -> Boolean) = apply { this.onContentType = onContentType }
        fun onContentType(onContentType: java.util.function.Predicate<UnitOfWork>) = apply { this.onContentType = { uow -> onContentType.test(uow) } }
        fun compactRule(compactRule: CompactRule) = apply { this.compactRule = compactRule }
        fun queryRule(queryRule: QueryRule) = apply { this.queryRule = queryRule }
        fun toQueryRequest(toQueryRequest: suspend (UnitOfWork) -> QueryRequest?) = apply { this.toQueryRequest = toQueryRequest }
        fun toQueryRequest(toQueryRequest: java.util.function.Function<UnitOfWork, QueryRequest?>) = apply { this.toQueryRequest = { uow -> toQueryRequest.apply(uow) } }
        fun toBatchGetRequest(toBatchGetRequest: suspend (UnitOfWork) -> BatchGetItemRequest?) = apply { this.toBatchGetRequest = toBatchGetRequest }
        fun toBatchGetRequest(toBatchGetRequest: java.util.function.Function<UnitOfWork, BatchGetItemRequest?>) = apply { this.toBatchGetRequest = { uow -> toBatchGetRequest.apply(uow) } }
        fun toEvent(toEvent: suspend (UnitOfWork) -> Event?) = apply { this.toEvent = toEvent }
        fun toEventJava(toEvent: java.util.function.Function<UnitOfWork, Event?>) = apply { this.toEvent = { uow -> toEvent.apply(uow) } }
        fun encryptEvent(encryptEvent: suspend (UnitOfWork) -> UnitOfWork) = apply { this.encryptEvent = encryptEvent }
        fun encryptEvent(encryptEvent: java.util.function.Function<UnitOfWork, UnitOfWork>) = apply { this.encryptEvent = { uow -> encryptEvent.apply(uow) } }
        fun parallel(parallel: Int) = apply { this.parallel = parallel }

        override fun build(): CdcPipeline {
            return CdcPipeline(
                id = id ?: throw IllegalArgumentException("id is required"),
                dynamoDbConnectorOptions = dynamoDbConnectorOptions,
                eventPublisher = eventPublisher ?: throw IllegalArgumentException("eventPublisher is required"),
                eventFilter = eventFilter,
                onContentType = onContentType,
                compactRule = compactRule,
                queryRule = queryRule,
                toQueryRequest = toQueryRequest,
                toBatchGetRequest = toBatchGetRequest,
                toEvent = toEvent,
                encryptEvent = encryptEvent,
                parallel = parallel
            )
        }
    }
}