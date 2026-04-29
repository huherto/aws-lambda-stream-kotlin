package io.github.huherto.awsLambdaStream.sinks

import aws.sdk.kotlin.services.dynamodb.DynamoDbClient
import io.github.huherto.awsLambdaStream.EnvironmentConfig
import io.github.huherto.awsLambdaStream.FaultManager
import io.github.huherto.awsLambdaStream.UnitOfWork
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.buffer

/**
 * DynamoDB-backed implementation of [EventsMicrostore].
 *
 * `EventsMicrostoreImpl` persists events from a stream of [UnitOfWork] items into the configured
 * DynamoDB events table, and can query previously stored events by partition key for correlation
 * workflows.
 *
 * The table name is resolved from [EnvironmentConfig.tableName]. If no table name is configured,
 * this implementation falls back to `"events"`.
 *
 * Typical usage:
 *
 * ```kotlin
 * val microstore = EventsMicrostoreImpl(
 *     envConfig = envConfig,
 *     dynamoDbClient = dynamoDbClient,
 *     faultManager = faultManager,
 * )
 *
 * val saved: Flow<UnitOfWork> = microstore.save(inputFlow)
 * val correlated: Flow<UnitOfWork> = microstore.queryByPk(queryFlow)
 * ```
 *
 * ## Saving events
 *
 * [save] expects each [UnitOfWork] to contain enough event data for the base microstore logic to
 * build a DynamoDB `PutItem` request. The returned flow contains the original unit of work enriched
 * with the DynamoDB put response when the write succeeds.
 *
 * ```kotlin
 * val persisted = microstore.save(flowOf(unitOfWork))
 * ```
 *
 * ## Querying correlated events
 *
 * [queryByPk] expects each [UnitOfWork] to contain query parameters that can be converted into a
 * DynamoDB `Query` request. The returned flow contains units of work enriched with the query
 * response and then converted into correlated event data.
 *
 * ```kotlin
 * val correlated = microstore.queryByPk(flowOf(unitOfWorkWithQueryParams))
 * ```
 *
 * ## Fault handling and buffering
 *
 * All transformations are executed through [FaultManager.mapNotFaulty], so faulty units of work are
 * skipped or handled according to the configured fault manager behavior. DynamoDB operations are
 * buffered using [bufferCapacity], which defaults to [Channel.BUFFERED].
 *
 * @param envConfig Provides environment configuration, including the DynamoDB table name.
 * @param dynamoDbClient AWS SDK DynamoDB client used for `PutItem` and `Query` calls.
 * @param faultManager Fault handling strategy used while processing flows.
 * @param bufferCapacity Coroutine flow buffer capacity between request-building and DynamoDB calls.
 */
open class EventsMicrostoreImpl(
    envConfig: EnvironmentConfig,
    private val dynamoDbClient: DynamoDbClient,
    faultManager: FaultManager,
    bufferCapacity: Int = Channel.BUFFERED,
): BaseEventsMicrostore(faultManager, bufferCapacity, envConfig.tableName() ?: "events") {

    override fun save(flow: Flow<UnitOfWork>) : Flow<UnitOfWork> {
        with(faultManager) {
            return flow.mapNotFaulty{ uow -> putRequest(uow) }
                .buffer(bufferCapacity)
                .mapNotFaulty { uow -> putDynamoDb(uow)  }
        }
    }

    override fun queryByPk(flow: Flow<UnitOfWork>) : Flow<UnitOfWork> {
        with(faultManager) {
            return flow.mapNotFaulty{ uow -> toQueryRequest(uow) }
                .buffer(bufferCapacity)
                .mapNotFaulty { uow -> queryDynamoDb(uow) }
                .mapNotFaulty { uow -> toCorrelated(uow) }
        }
    }

    private val putDynamoDb: suspend (UnitOfWork) -> UnitOfWork = { uow ->
        val putResponse = uow.putRequest?.let {
            dynamoDbClient.putItem(uow.putRequest)
        }
        uow.copy(putResponse = putResponse)
    }

    private val queryDynamoDb: suspend (UnitOfWork) -> UnitOfWork = { uow ->
        val queryResponse = uow.queryRequest?.let {
            dynamoDbClient.query(uow.queryRequest)
        }
        uow.copy(queryResponse = queryResponse)
    }

}