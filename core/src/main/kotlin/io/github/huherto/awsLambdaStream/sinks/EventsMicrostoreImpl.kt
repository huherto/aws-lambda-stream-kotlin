package io.github.huherto.awsLambdaStream.sinks

import aws.sdk.kotlin.services.dynamodb.DynamoDbClient
import io.github.huherto.awsLambdaStream.EnvironmentConfig
import io.github.huherto.awsLambdaStream.FaultManager
import io.github.huherto.awsLambdaStream.GlobalRegistry
import io.github.huherto.awsLambdaStream.GlobalRegistry.envConfig
import io.github.huherto.awsLambdaStream.UnitOfWork
import io.github.huherto.awsLambdaStream.connectors.DynamoDbClientFactory
import io.github.huherto.awsLambdaStream.extensions.putRequest
import io.github.huherto.awsLambdaStream.extensions.queryRequest
import io.github.huherto.awsLambdaStream.extensions.withPutResponse
import io.github.huherto.awsLambdaStream.extensions.withQueryResponse
import io.github.huherto.awsLambdaStream.metrics.withStepMetrics
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
 * val saved: Flow<ReplayUnitOfWork> = microstore.save(inputFlow)
 * val correlated: Flow<ReplayUnitOfWork> = microstore.queryByPk(queryFlow)
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
 * All transformations are executed through [FaultManager.mapNotFaultyFrom], so faulty units of work are
 * skipped or handled according to the configured fault manager behavior. DynamoDB operations are
 * buffered using [bufferCapacity], which defaults to [Channel.BUFFERED].
 *
 * @param dynamoDbClientFactory A factory to create AWS SDK DynamoDB client used for `PutItem` and `Query` calls.
 * @param faultManager Fault handling strategy used while processing flows.
 * @param bufferCapacity Coroutine flow buffer capacity between request-building and DynamoDB calls.
 */
open class EventsMicrostoreImpl(
    private val dynamoDbClientFactory: DynamoDbClientFactory,
    faultManager: FaultManager = GlobalRegistry.faultManager(),
    bufferCapacity: Int = Channel.BUFFERED,
): BaseEventsMicrostore(faultManager, bufferCapacity, envConfig().tableName() ?: "events") {

    override fun save(flow: Flow<UnitOfWork>) : Flow<UnitOfWork> {
        with(faultManager) {
            return flow.mapNotFaulty{ uow -> putRequest(uow) }
                .buffer(bufferCapacity)
                .mapNotFaulty { uow ->
                    uow.withStepMetrics("save") { uowWithMetrics ->
                        putDynamoDb(uowWithMetrics)
                    }
                }
        }
    }

    override fun queryByPk(flow: Flow<UnitOfWork>) : Flow<UnitOfWork> {
        with(faultManager) {
            return flow.mapNotFaulty{ uow -> toQueryRequest(uow) }
                .buffer(bufferCapacity)
                .mapNotFaulty { uow ->
                    uow.withStepMetrics("query") { uowWithMetrics ->
                        queryDynamoDb(uowWithMetrics)
                    }
                }
                .mapNotFaulty { uow -> toCorrelated(uow) }
        }
    }

    private fun getClient(uow: UnitOfWork) : DynamoDbClient {
        val pipelineId = uow.pipeline?.id ?: "unknown"
        return dynamoDbClientFactory.getClient(pipelineId)
    }

    private suspend fun putDynamoDb(uow: UnitOfWork): UnitOfWork {
        val client = getClient(uow)
        val putResponse = uow.putRequest?.let {
            client.putItem(it)
        }
        return uow.withPutResponse(putResponse)
    }

    private suspend fun queryDynamoDb(uow: UnitOfWork): UnitOfWork {
        val client = getClient(uow)
        val queryResponse = uow.queryRequest?.let {
            client.query(it)
        }
        return uow.withQueryResponse(queryResponse)
    }

}