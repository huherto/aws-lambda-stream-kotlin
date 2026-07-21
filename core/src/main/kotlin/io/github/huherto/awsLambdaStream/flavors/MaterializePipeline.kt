package io.github.huherto.awsLambdaStream.flavors

import aws.sdk.kotlin.services.dynamodb.model.UpdateItemRequest
import io.github.huherto.awsLambdaStream.EnvironmentConfig
import io.github.huherto.awsLambdaStream.FaultManager
import io.github.huherto.awsLambdaStream.UnitOfWork
import io.github.huherto.awsLambdaStream.connectors.DynamoDbConnector
import io.github.huherto.awsLambdaStream.filters.EventFilter
import io.github.huherto.awsLambdaStream.filters.filterEvents
import io.github.huherto.awsLambdaStream.filters.outSourceIsSelf
import io.github.huherto.awsLambdaStream.sinks.DynamoDbSink
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.onEach

class MaterializePipeline(
    pipelineId: String,
    envConfig: EnvironmentConfig,
    private val eventFilter: EventFilter = EventFilter.Any,
    private val onContentType: (UnitOfWork) -> Boolean = { true },
    private val compact: (Flow<UnitOfWork>) -> Flow<UnitOfWork> = { it },
    private val toUpdateRequest: suspend (UnitOfWork) -> UpdateItemRequest?,
    private val dynamoDbConnector: DynamoDbConnector,
) : Pipeline(pipelineId, envConfig) {

    private val dynamoDbSink: DynamoDbSink by lazy { DynamoDbSink(envConfig, dynamoDbConnector) }

    override fun connect(
        fm: FaultManager,
        fromFlow: Flow<UnitOfWork>,
    ): Flow<UnitOfWork> {
        logger.info { "MaterializePipeline.connect: id=$id" }

        with(fm) {
            return fromFlow
                .filterNotFaulty { uow -> outSourceIsSelf(envConfig, uow) }
                .filterEvents(fm, eventFilter)
                .onEach { uow -> printStartPipeline(uow) }
                .filterNotFaulty { uow -> onContentType(uow) }
                .let { flow -> compact(flow) }
                .mapNotFaulty { uow ->
                    uow.copy(
                        updateRequest = toUpdateRequest(uow),
                    )
                }
                .let { flow -> dynamoDbSink.update(this, flow) }
                .onEach { uow -> printEndPipeline(uow) }
        }
    }
}