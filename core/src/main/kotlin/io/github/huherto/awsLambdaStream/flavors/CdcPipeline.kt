package io.github.huherto.awsLambdaStream.flavors

import aws.sdk.kotlin.services.dynamodb.DynamoDbClient
import aws.sdk.kotlin.services.dynamodb.model.BatchGetItemRequest
import aws.sdk.kotlin.services.dynamodb.model.QueryRequest
import io.github.huherto.awsLambdaStream.Event
import io.github.huherto.awsLambdaStream.FaultManager
import io.github.huherto.awsLambdaStream.UnitOfWork
import io.github.huherto.awsLambdaStream.filters.EventFilter
import io.github.huherto.awsLambdaStream.filters.filterEvents
import io.github.huherto.awsLambdaStream.filters.outLatched
import io.github.huherto.awsLambdaStream.queries.Rule
import io.github.huherto.awsLambdaStream.queries.batchGetDynamoDB
import io.github.huherto.awsLambdaStream.queries.queryAllDynamoDB
import io.github.huherto.awsLambdaStream.queries.toPkQueryRequest
import io.github.huherto.awsLambdaStream.sinks.EventPublisher
import io.github.huherto.awsLambdaStream.utils.CompactRule
import io.github.huherto.awsLambdaStream.utils.compact
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.buffer
import kotlinx.coroutines.flow.onEach

/**
 * Pipeline flavor for change-data-capture style processing.
 *
 * A `CdcPipeline` receives incoming [UnitOfWork] items, filters them, optionally compacts related
 * records, queries related DynamoDB state, optionally enriches the event, and publishes the resulting
 * event.
 *
 * Processing order:
 * 1. Ignore events latched from this same source.
 * 2. Apply the configured event filter.
 * 3. Log pipeline start.
 * 4. Apply content filtering.
 * 5. Compact the stream when configured.
 * 6. Build and execute a DynamoDB query request.
 * 7. Build and execute a DynamoDB batch-get request.
 * 8. Optionally transform/enrich the event.
 * 9. Buffer downstream work using [parallel].
 * 10. Optionally encrypt or otherwise transform the event.
 * 11. Publish the resulting event.
 * 12. Log pipeline completion.
 *
 * @param id Unique identifier for this pipeline.
 * @param dynamoDbClient Client used for query and batch-get stages.
 * @param eventPublisher Sink responsible for publishing final events.
 * @param eventFilter Event-level filter applied before pipeline-specific processing starts.
 * @param onContentType Predicate used to accept or reject a [UnitOfWork] after event filtering.
 * @param compactRule Optional stream compaction rule.
 * @param queryRule Rule used by the default primary-key query request builder.
 * @param queryRelated Whether the default primary-key query request should be created.
 * @param toQueryRequest Optional custom query request mapper.
 * @param toGetRequest Optional custom batch-get request mapper.
 * @param toEvent Optional event enrichment/translation function.
 * @param encryptEvent Optional final event transformation before publishing.
 * @param parallel Buffer capacity used before publish.
 */
class CdcPipeline(
    id: String,
    private val dynamoDbClient: DynamoDbClient,
    private val eventPublisher: EventPublisher,
    private val eventFilter: EventFilter = EventFilter.Any,
    private val onContentType: (UnitOfWork) -> Boolean = { true },
    private val compactRule: CompactRule? = null,
    private val queryRule: Rule,
    private val queryRelated: Boolean = true,
    private val toQueryRequest: (suspend (UnitOfWork) -> QueryRequest?)? = null,
    private val toGetRequest: (suspend (UnitOfWork) -> BatchGetItemRequest?)? = null,
    private val toEvent: (suspend (UnitOfWork) -> Event?)? = null,
    private val encryptEvent: (suspend (UnitOfWork) -> UnitOfWork)? = null,
    private val parallel: Int = System.getenv("PARALLEL")?.toIntOrNull() ?: 4,
) : Pipeline(id) {

    /**
     * Builds the DynamoDB query request for the current unit of work.
     *
     * This follows the TypeScript behavior:
     * - use a custom mapper when supplied;
     * - return `null` when related queries are disabled;
     * - otherwise create the default primary-key query request.
     */
    internal suspend fun addQueryRequest(uow: UnitOfWork): UnitOfWork {
        val queryRequest = when {
            toQueryRequest != null -> toQueryRequest.invoke(uow)
            !queryRelated -> null
            else -> toPkQueryRequest(uow, queryRule)
        }

        return uow.copy(queryRequest = queryRequest)
    }

    /**
     * Builds the optional DynamoDB batch-get request for the current unit of work.
     */
    internal suspend fun addBatchGetRequest(uow: UnitOfWork): UnitOfWork {
        return uow.copy(
            batchGetRequest = toGetRequest?.invoke(uow)
        )
    }

    /**
     * Applies the optional event transformation.
     *
     * If no transformation is configured, the unit of work passes through unchanged.
     */
    internal suspend fun addEvent(uow: UnitOfWork): UnitOfWork {
        val mapper = toEvent ?: return uow
        return uow.copy(event = mapper(uow))
    }

    /**
     * Applies optional event encryption/transformation.
     *
     * The original TypeScript version calls `encryptEvent(...)` immediately before publishing.
     * In Kotlin this is injected as a suspending unit-of-work mapper so callers can provide the
     * desired encryption implementation.
     */
    internal suspend fun encrypt(uow: UnitOfWork): UnitOfWork {
        return encryptEvent?.invoke(uow) ?: uow
    }

    /**
     * Publishes final units of work through the configured [eventPublisher].
     */
    internal fun Flow<UnitOfWork>.publish(): Flow<UnitOfWork> {
        return eventPublisher.publish(this)
    }

    /**
     * Connects this CDC pipeline to an upstream [Flow].
     */
    @OptIn(ExperimentalCoroutinesApi::class)
    override fun connect(
        fm: FaultManager,
        fromFlow: Flow<UnitOfWork>,
    ): Flow<UnitOfWork> {
        logger.info { "CdcPipeline.connect: id=$id" }

        with(fm) {
            return fromFlow
                .filterNotFaulty { uow -> outLatched(uow) }
                .filterEvents(fm, eventFilter)
                .onEach { uow -> printStartPipeline(uow) }
                .filterNotFaulty { uow -> onContentType(uow) }
                .compact(compactRule)
                .mapNotFaulty { uow -> addQueryRequest(uow) }
                .queryAllDynamoDB(dynamoDbClient)
                .mapNotFaulty { uow -> addBatchGetRequest(uow) }
                .batchGetDynamoDB(dynamoDbClient)
                .mapNotFaulty { uow -> addEvent(uow) }
                .buffer(parallel)
                .mapNotFaulty { uow -> encrypt(uow) }
                .publish()
                .onEach { uow -> printEndPipeline(uow) }
        }
    }
}