package io.github.huherto.awsLambdaStream.flavors

import aws.sdk.kotlin.services.dynamodb.model.UpdateItemRequest
import io.github.huherto.awsLambdaStream.PipelineBuilder
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
    private val eventFilter: EventFilter,
    private val onContentType: (UnitOfWork) -> Boolean,
    private val compact: (Flow<UnitOfWork>) -> Flow<UnitOfWork>,
    private val toUpdateRequest: suspend (UnitOfWork) -> UpdateItemRequest?,
    private val dynamoDbConnectorOptions: DynamoDbConnector.Options,
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

    companion object {
        @JvmStatic
        fun builder() = Builder()
    }

    class Builder : PipelineBuilder<MaterializePipeline, Builder>() {
        private var eventFilter: EventFilter = EventFilter.Any
        private var onContentType: (UnitOfWork) -> Boolean = { true }
        private var compact: (Flow<UnitOfWork>) -> Flow<UnitOfWork> = { it }
        private var toUpdateRequest: (suspend (UnitOfWork) -> UpdateItemRequest?)? = null
        private var dynamoDbConnectorOptions: DynamoDbConnector.Options = DynamoDbConnector.Options()

        fun eventFilter(eventFilter: EventFilter) = apply { this.eventFilter = eventFilter }
        fun onContentType(onContentType: (UnitOfWork) -> Boolean) = apply { this.onContentType = onContentType }
        fun onContentType(onContentType: java.util.function.Predicate<UnitOfWork>) = apply { this.onContentType = { uow -> onContentType.test(uow) } }
        fun compact(compact: (Flow<UnitOfWork>) -> Flow<UnitOfWork>) = apply { this.compact = compact }
        fun compact(compact: java.util.function.Function<Flow<UnitOfWork>, Flow<UnitOfWork>>) = apply { this.compact = { flow -> compact.apply(flow) } }
        fun toUpdateRequest(toUpdateRequest: suspend (UnitOfWork) -> UpdateItemRequest?) = apply { this.toUpdateRequest = toUpdateRequest }
        fun toUpdateRequest(toUpdateRequest: java.util.function.Function<UnitOfWork, UpdateItemRequest?>) = apply { this.toUpdateRequest = { uow -> toUpdateRequest.apply(uow) } }
        fun dynamoDbConnectorOptions(options: DynamoDbConnector.Options) = apply { this.dynamoDbConnectorOptions = options }

        override fun build(): MaterializePipeline {
            return MaterializePipeline(
                pipelineId = id ?: throw IllegalArgumentException("id is required"),
                eventFilter = eventFilter,
                onContentType = onContentType,
                compact = compact,
                toUpdateRequest = toUpdateRequest ?: throw IllegalArgumentException("toUpdateRequest is required"),
                dynamoDbConnectorOptions = dynamoDbConnectorOptions
            )
        }
    }
}