package io.github.huherto.awsLambdaStream.sinks

import aws.sdk.kotlin.services.dynamodb.DynamoDbClient
import io.github.huherto.awsLambdaStream.EnvironmentConfig
import io.github.huherto.awsLambdaStream.FaultManager
import io.github.huherto.awsLambdaStream.UnitOfWork
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.buffer
import kotlinx.coroutines.flow.mapNotNull

open class EventsMicrostoreImpl constructor(
    envConfig: EnvironmentConfig,
    private val dynamoDbClient: DynamoDbClient,
    faultManager: FaultManager,
    bufferCapacity: Int = Channel.Factory.BUFFERED,
): BaseEventsMicrostore(faultManager, bufferCapacity, envConfig.tableName() ?: "events") {

    override fun save(flow: Flow<UnitOfWork>) : Flow<UnitOfWork> {
        with(faultManager) {
            return flow.mapNotFaulty{ uow -> putRequest(uow) }
                .buffer(bufferCapacity)
                .mapNotNull { uow -> faulty(uow) { putDynamoDb(uow) } }
        }
    }

    override fun queryByPk(flow: Flow<UnitOfWork>) : Flow<UnitOfWork> {
        with(faultManager) {
            return flow.mapNotFaulty{ uow -> toQueryRequest(uow) }
                .buffer(bufferCapacity)
                .mapNotNull { uow -> faulty(uow) { queryDynamoDb(uow)} }
                .mapNotNull { uow -> faulty(uow) { toCorrelated(uow) } }
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