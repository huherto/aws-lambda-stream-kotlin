package io.github.huherto.awsLambdaStream.flavors

import aws.sdk.kotlin.services.dynamodb.model.BatchGetItemRequest
import aws.sdk.kotlin.services.dynamodb.model.QueryRequest
import io.github.huherto.awsLambdaStream.Event
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
    private val dynamoDbConnectorOptions: DynamoDbConnector.Options = DynamoDbConnector.Options(),
    private val eventPublisher: EventPublisher,
    private val eventFilter: EventFilter = EventFilter.Any,
    private val onContentType: (UnitOfWork) -> Boolean = { true },
    private val compactRule: CompactRule? = null,
    private val queryRule: QueryRule? = null,
    private val toQueryRequest: (suspend (UnitOfWork) -> QueryRequest?)? = null,
    private val toBatchGetRequest: (suspend (UnitOfWork) -> BatchGetItemRequest?)? = null,
    private val toEvent: (suspend (UnitOfWork) -> Event?)? = null,
    private val encryptEvent: (suspend (UnitOfWork) -> UnitOfWork)? = null,
    private val parallel: Int = System.getenv("PARALLEL")?.toIntOrNull() ?: 4,
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

}