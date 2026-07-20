package org.myorg.sut

import io.github.huherto.awsLambdaStream.EnvironmentConfig
import io.github.huherto.awsLambdaStream.FaultManager
import io.github.huherto.awsLambdaStream.JsonEventCodec
import io.github.huherto.awsLambdaStream.PipelineAssembler
import io.github.huherto.awsLambdaStream.connectors.DynamoDbConnector
import io.github.huherto.awsLambdaStream.flavors.Pipeline
import io.github.huherto.awsLambdaStream.flavors.UpdatePipeline
import io.github.huherto.awsLambdaStream.from.KinesisAdapter
import io.github.huherto.awsLambdaStream.sinks.DynamoDbSink
import io.github.huherto.awsLambdaStream.sinks.EventBridgePublisher
import mu.KotlinLogging.logger

class KinesisTriggerContainer (
    val envConfig: EnvironmentConfig,
    val faultManager: FaultManager,
    val dynamoDbConnector: DynamoDbConnector? = null,
) {

    companion object {

        private val logger = logger {}

        fun build() : KinesisTriggerContainer {
            val envConfig = EnvironmentConfig()
            val eventPublisher = EventBridgePublisher(envConfig)
            val faultManager = FaultManager(envConfig, eventPublisher)
                //val dynamoDbClientFactory = DefaultDynamoDbClientFactory(envConfig)
            //val dynamoDbConnector = DynamoDbConnector(dynamoDbClientFactory = dynamoDbClientFactory)
            val dynamoDbSink = DynamoDbSink(envConfig = envConfig)

            return KinesisTriggerContainer(
                envConfig = envConfig,
                faultManager = faultManager,
            )
        }
    }

    private  val updatePipeline: Pipeline by lazy {
        UpdatePipeline(
            id = "update",
            envConfig = envConfig,
            dynamoDbConnector = dynamoDbConnector,
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