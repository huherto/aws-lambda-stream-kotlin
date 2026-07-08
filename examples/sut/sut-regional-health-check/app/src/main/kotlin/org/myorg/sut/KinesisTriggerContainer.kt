package org.myorg.sut

import io.github.huherto.awsLambdaStream.EnvironmentConfig
import io.github.huherto.awsLambdaStream.FaultManager
import io.github.huherto.awsLambdaStream.JsonEventCodec
import io.github.huherto.awsLambdaStream.PipelineAssembler
import io.github.huherto.awsLambdaStream.connectors.DefaultDynamoDbClientFactory
import io.github.huherto.awsLambdaStream.connectors.DynamoDbConnector
import io.github.huherto.awsLambdaStream.flavors.Pipeline
import io.github.huherto.awsLambdaStream.flavors.UpdatePipeline
import io.github.huherto.awsLambdaStream.from.KinesisAdapter
import io.github.huherto.awsLambdaStream.sinks.DynamoDbSink
import io.github.huherto.awsLambdaStream.sinks.EventBridgePublisher
import mu.KotlinLogging.logger

class KinesisTriggerContainer (
    val faultManager: FaultManager,
    val dynamoDbConnector: DynamoDbConnector,
    val dynamoDbSink: DynamoDbSink,
) {

    companion object {

        private val logger = logger {}

        fun build() : KinesisTriggerContainer {
            val envConfig = EnvironmentConfig()
            val eventPublisher = EventBridgePublisher(envConfig)
            val faultManager = FaultManager(envConfig, eventPublisher)
            val dynamoDbClientFactory = DefaultDynamoDbClientFactory(envConfig)
            val dynamoDbConnector = DynamoDbConnector(dynamoDbClientFactory = dynamoDbClientFactory)
            val dynamoDbSink = DynamoDbSink(
                envConfig = envConfig,
                connector = dynamoDbConnector)

            return KinesisTriggerContainer(
                faultManager = faultManager,
                dynamoDbConnector = dynamoDbConnector,
                dynamoDbSink = dynamoDbSink,
            )
        }
    }

    private  val updatePipeline: Pipeline by lazy {
        UpdatePipeline(
            id = "update",
            dynamoDbConnector = dynamoDbConnector,
            dynamoDbSink = dynamoDbSink,
            eventCodec = JsonEventCodec,
            toUpdateRequest = ::toUpdateRequest
        )
    }

    val assembler: PipelineAssembler by lazy {
        PipelineAssembler
            .builder()
            .addPipeline(updatePipeline)
            .faultManager(faultManager)
            .build()
    }

    val kinesisAdapter = KinesisAdapter(faultManager, eventCodec = JsonEventCodec)

}