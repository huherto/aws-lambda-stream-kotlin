package org.myorg.sut

import aws.sdk.kotlin.services.dynamodb.model.UpdateItemRequest
import io.github.huherto.awsLambdaStream.EnvironmentConfig
import io.github.huherto.awsLambdaStream.FaultManager
import io.github.huherto.awsLambdaStream.PipelineAssembler
import io.github.huherto.awsLambdaStream.UnitOfWork
import io.github.huherto.awsLambdaStream.connectors.DefaultDynamoDbClientFactory
import io.github.huherto.awsLambdaStream.connectors.DynamoDbConnector
import io.github.huherto.awsLambdaStream.filters.EventFilters
import io.github.huherto.awsLambdaStream.flavors.MaterializePipeline
import io.github.huherto.awsLambdaStream.flavors.Pipeline
import io.github.huherto.awsLambdaStream.from.KinesisAdapter
import io.github.huherto.awsLambdaStream.sinks.EventBridgePublisher

class ListenerContainer(
    val envConfig: EnvironmentConfig,
    val dynamoDbConnector: DynamoDbConnector,
    val faultManager: FaultManager,
) {

    companion object {
        fun build() : ListenerContainer {
            val envConfig = EnvironmentConfig()
            val dynamoDbClientFactory = DefaultDynamoDbClientFactory(envConfig)
            val dynamoDbConnector = DynamoDbConnector(dynamoDbClientFactory = dynamoDbClientFactory)
            val eventPublisher = EventBridgePublisher(envConfig)
            val faultManager = FaultManager(envConfig, eventPublisher)
            return ListenerContainer(
                envConfig = envConfig,
                dynamoDbConnector = dynamoDbConnector,
                faultManager = faultManager
            )
        }
    }

    val kinesisAdapter: KinesisAdapter by lazy {
        KinesisAdapter(
            faultManager = faultManager,
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
            envConfig = envConfig,
            eventFilter = EventFilters.classes(TrackedUnitEvent::class),
            toUpdateRequest = ::toUpdateRequest,
            dynamoDbConnector = dynamoDbConnector,
        )
    }

    val assembler: PipelineAssembler by lazy {
        PipelineAssembler
            .builder()
            .faultManager(faultManager)
            .addPipeline(materializePipeline)
            .build()
    }

}