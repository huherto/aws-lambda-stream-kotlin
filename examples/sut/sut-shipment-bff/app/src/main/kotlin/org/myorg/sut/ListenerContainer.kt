package org.myorg.sut

import aws.sdk.kotlin.services.dynamodb.model.UpdateItemRequest
import io.github.huherto.awsLambdaStream.PipelineAssembler
import io.github.huherto.awsLambdaStream.UnitOfWork
import io.github.huherto.awsLambdaStream.connectors.DefaultDynamoDbClientFactory
import io.github.huherto.awsLambdaStream.connectors.DynamoDbConnector
import io.github.huherto.awsLambdaStream.filters.EventFilters
import io.github.huherto.awsLambdaStream.flavors.MaterializePipeline
import io.github.huherto.awsLambdaStream.flavors.Pipeline
import io.github.huherto.awsLambdaStream.from.KinesisAdapter

class ListenerContainer(
    val dynamoDbConnector: DynamoDbConnector,
) {

    companion object {
        fun build() : ListenerContainer {
            val dynamoDbClientFactory = DefaultDynamoDbClientFactory()
            val dynamoDbConnector = DynamoDbConnector(dynamoDbClientFactory = dynamoDbClientFactory)
            return ListenerContainer(
                dynamoDbConnector = dynamoDbConnector,
            )
        }
    }

    val kinesisAdapter: KinesisAdapter by lazy {
        KinesisAdapter(
            eventCodec = TrackedUnitEventCodec,
        )
    }

    suspend fun toUpdateRequest(uow: UnitOfWork): UpdateItemRequest? {

        val event = uow.event as? TrackedUnitEvent ?: return null
        val entity = event.entity ?: return null
        // No rules implemented yet.
        return null
    }

    private val materializePipeline: Pipeline by lazy {
        MaterializePipeline(
            pipelineId = "m1",
            eventFilter = EventFilters.classes(TrackedUnitEvent::class),
            toUpdateRequest = ::toUpdateRequest,
            dynamoDbConnector = dynamoDbConnector,
        )
    }

    val assembler: PipelineAssembler by lazy {
        PipelineAssembler
            .builder()
            .addPipeline(materializePipeline)
            .build()
    }

}