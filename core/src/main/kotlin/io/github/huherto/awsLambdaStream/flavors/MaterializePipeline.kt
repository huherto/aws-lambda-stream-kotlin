package io.github.huherto.awsLambdaStream.flavors

import aws.sdk.kotlin.services.dynamodb.model.UpdateItemRequest
import io.github.huherto.awsLambdaStream.UnitOfWork
import io.github.huherto.awsLambdaStream.connectors.DynamoDbConnector
import io.github.huherto.awsLambdaStream.extensions.withUpdateRequest
import io.github.huherto.awsLambdaStream.faults.FaultManager
import io.github.huherto.awsLambdaStream.filters.EventFilter
import io.github.huherto.awsLambdaStream.filters.filterEvents
import io.github.huherto.awsLambdaStream.filters.outSourceIsSelf
import io.github.huherto.awsLambdaStream.sinks.DynamoDbSink
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.onEach

/** Pipeline flavor for materializing events into DynamoDB records. */
class MaterializePipeline(
    pipelineId: String,
    private val eventFilter: EventFilter = EventFilter.Any,
    private val onContentType: (UnitOfWork) -> Boolean = { true },
    private val compact: (Flow<UnitOfWork>) -> Flow<UnitOfWork> = { it },
    private val toUpdateRequest: suspend (UnitOfWork) -> UpdateItemRequest?,
    private val dynamoDbConnectorOptions: DynamoDbConnector.Options = DynamoDbConnector.Options(),
) : Pipeline(pipelineId) {

    private val dynamoDbSink: DynamoDbSink by lazy { DynamoDbSink(DynamoDbConnector(dynamoDbConnectorOptions)) }

    override fun connect(
        fm: FaultManager,
        fromFlow: Flow<UnitOfWork>,
    ): Flow<UnitOfWork> {
        logger.info { "MaterializePipeline.connect: id=$id" }

        with(fm) {
            return fromFlow
                .filterNotFaulty { uow -> outSourceIsSelf(uow) }
                .filterEvents(fm, eventFilter)
                .onEach { uow -> printStartPipeline(uow) }
                .filterNotFaulty { uow -> onContentType(uow) }
                .let { flow -> compact(flow) }
                .mapNotFaulty { uow ->
                    uow.withUpdateRequest(toUpdateRequest(uow))
                }
                .let { flow -> dynamoDbSink.update(this, flow) }
                .onEach { uow -> printEndPipeline(uow) }
        }
    }
}