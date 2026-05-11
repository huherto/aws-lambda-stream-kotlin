package org.myorg.sut

import io.github.huherto.awsLambdaStream.EnvironmentConfig
import io.github.huherto.awsLambdaStream.FaultManager
import io.github.huherto.awsLambdaStream.PipelineAssembler
import io.github.huherto.awsLambdaStream.connectors.DefaultDynamoDbClientFactory
import io.github.huherto.awsLambdaStream.connectors.DynamoDbConnector
import io.github.huherto.awsLambdaStream.filters.EventFilters
import io.github.huherto.awsLambdaStream.flavors.MaterializePipeline
import io.github.huherto.awsLambdaStream.flavors.Pipeline
import io.github.huherto.awsLambdaStream.from.KinesisAdapter
import io.github.huherto.awsLambdaStream.sinks.EventBridgePublishOptions
import io.github.huherto.awsLambdaStream.sinks.EventBridgePublisher
import java.nio.ByteBuffer

class ListenerContainer(
    val envConfig: EnvironmentConfig,
    val dynamoDbConnector: DynamoDbConnector,
    val faultManager: FaultManager,
) {

    companion object {
        fun build() : ListenerContainer {
            val envConfig = EnvironmentConfig()
            val dynamoDbClientFactory = DefaultDynamoDbClientFactory(envConfig)
            val dynamoDbConnector = DynamoDbConnector(clientFactory = dynamoDbClientFactory)
            val eventPublisherOptions = EventBridgePublishOptions(envConfig)
            val eventPublisher = EventBridgePublisher(eventPublisherOptions)
            val faultManager = FaultManager(envConfig, eventPublisher)
            return ListenerContainer(
                envConfig = envConfig,
                dynamoDbConnector = dynamoDbConnector,
                faultManager = faultManager
            )
        }
    }

    class MyKinesisAdapter : KinesisAdapter() {
        override fun decodePayload(payload: ByteBuffer?): TrackedUnitEvent {
            return sutJson.decodeFromString<TrackedUnitEvent>(utf8Decode(payload))
        }
    }

    val kinesisAdapter: KinesisAdapter by lazy { MyKinesisAdapter() }

    private val materializePipeline: Pipeline by lazy {
        MaterializePipeline(
            pipelineId = "m1",
            envConfig = envConfig,
            eventFilter = EventFilters.classes(TrackedUnitEvent::class),
            toUpdateRequest = { null }, // TODO: Implement
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