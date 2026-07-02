package org.myorg.sut

import io.github.huherto.awsLambdaStream.EnvironmentConfig
import io.github.huherto.awsLambdaStream.FaultManager
import io.github.huherto.awsLambdaStream.PipelineAssembler
import io.github.huherto.awsLambdaStream.connectors.DefaultS3ClientFactory
import io.github.huherto.awsLambdaStream.connectors.S3Connector
import io.github.huherto.awsLambdaStream.flavors.MaterializeS3Pipeline
import io.github.huherto.awsLambdaStream.flavors.Pipeline
import io.github.huherto.awsLambdaStream.from.DynamodbAdapter
import io.github.huherto.awsLambdaStream.sinks.EventBridgePublisher
import mu.KotlinLogging.logger

class DynamoDbTriggerContainer (
    val envConfig: EnvironmentConfig,
    val faultManager: FaultManager,
    val s3Connector: S3Connector,
) {

    companion object {

        private val logger = logger {}

        fun build() : DynamoDbTriggerContainer {
            val envConfig = EnvironmentConfig()
            val eventPublisher = EventBridgePublisher(envConfig)
            val faultManager = FaultManager(envConfig, eventPublisher)
            val defaultS3ClientFactory = DefaultS3ClientFactory(envConfig)
            val s3Connector = S3Connector(defaultS3ClientFactory)

            return DynamoDbTriggerContainer(
                envConfig = envConfig,
                faultManager = faultManager,
                s3Connector = s3Connector,
            )
        }
    }

    private  val materializeS3Pipeline: Pipeline by lazy {
        MaterializeS3Pipeline(
            pipelineId = "t1",
            envConfig = envConfig,
            s3Connector = s3Connector,
            toPutRequest = ::toS3PutRequest
        )
    }

    val assembler: PipelineAssembler by lazy {
        PipelineAssembler
            .builder()
            .addPipeline(materializeS3Pipeline)
            .faultManager(faultManager)
            .build()
    }

    val dynamoDbAdapter = DynamodbAdapter(faultManager)

}