package io.kopipes.aws.sinks

import aws.sdk.kotlin.services.dynamodb.DynamoDbClient
import io.kopipes.aws.AwsGlobalRegistry
import io.kopipes.aws.awsEnvConfig
import io.kopipes.aws.connectors.DynamoDbClientFactory
import io.kopipes.aws.extensions.putRequest
import io.kopipes.aws.extensions.queryRequest
import io.kopipes.aws.extensions.withPutResponse
import io.kopipes.aws.extensions.withQueryResponse
import io.kopipes.core.UnitOfWork
import io.kopipes.core.faults.FaultManager
import io.kopipes.core.metrics.withStepMetrics
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.buffer

/** DynamoDB-backed implementation of [EventsMicrostore]. */
open class EventsMicrostoreImpl @JvmOverloads constructor(
    private val dynamoDbClientFactory: DynamoDbClientFactory = AwsGlobalRegistry.dynamoDbClientFactory(),
    faultManager: FaultManager = AwsGlobalRegistry.faultManager(),
    bufferCapacity: Int = Channel.BUFFERED,
) : BaseEventsMicrostore(faultManager, bufferCapacity, awsEnvConfig().tableName() ?: "events") {

    override fun save(flow: Flow<UnitOfWork>): Flow<UnitOfWork> {
        with(faultManager) {
            return flow.mapNotFaulty { uow -> putRequest(uow) }
                .buffer(bufferCapacity)
                .mapNotFaulty { uow ->
                    uow.withStepMetrics("save") { uowWithMetrics ->
                        putDynamoDb(uowWithMetrics)
                    }
                }
        }
    }

    override fun queryByPk(flow: Flow<UnitOfWork>): Flow<UnitOfWork> {
        with(faultManager) {
            return flow.mapNotFaulty { uow -> toQueryRequest(uow) }
                .buffer(bufferCapacity)
                .mapNotFaulty { uow ->
                    uow.withStepMetrics("query") { uowWithMetrics ->
                        queryDynamoDb(uowWithMetrics)
                    }
                }
                .mapNotFaulty { uow -> toCorrelated(uow) }
        }
    }

    private fun getClient(uow: UnitOfWork): DynamoDbClient {
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
