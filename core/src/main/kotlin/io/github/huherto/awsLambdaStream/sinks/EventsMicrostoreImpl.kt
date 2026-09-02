package io.github.huherto.awsLambdaStream.sinks

import aws.sdk.kotlin.services.dynamodb.DynamoDbClient
import io.github.huherto.awsLambdaStream.GlobalRegistry
import io.github.huherto.awsLambdaStream.GlobalRegistry.envConfig
import io.github.huherto.awsLambdaStream.UnitOfWork
import io.github.huherto.awsLambdaStream.connectors.DynamoDbClientFactory
import io.github.huherto.awsLambdaStream.extensions.putRequest
import io.github.huherto.awsLambdaStream.extensions.queryRequest
import io.github.huherto.awsLambdaStream.extensions.withPutResponse
import io.github.huherto.awsLambdaStream.extensions.withQueryResponse
import io.github.huherto.awsLambdaStream.faults.FaultManager
import io.github.huherto.awsLambdaStream.metrics.withStepMetrics
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.buffer

/** DynamoDB-backed implementation of [EventsMicrostore]. */
open class EventsMicrostoreImpl @JvmOverloads constructor(
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