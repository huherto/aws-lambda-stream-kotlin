package org.myorg.sut

import io.github.huherto.awsLambdaStream.EnvironmentConfig
import io.github.huherto.awsLambdaStream.GlobalRegistry
import io.github.huherto.awsLambdaStream.JsonEventCodec
import io.github.huherto.awsLambdaStream.PipelineAssembler
import io.github.huherto.awsLambdaStream.connectors.DynamoDbConnector
import io.github.huherto.awsLambdaStream.flavors.Pipeline
import io.github.huherto.awsLambdaStream.flavors.UpdatePipeline
import io.github.huherto.awsLambdaStream.from.KinesisAdapter
import mu.KotlinLogging.logger

class KinesisTriggerContainer (
    val envConfig: EnvironmentConfig,
    val dynamoDbConnector: DynamoDbConnector? = null,
) {

    companion object {

        private val logger = logger {}

        fun build() : KinesisTriggerContainer {
            val envConfig = GlobalRegistry.envConfig()
            return KinesisTriggerContainer(
                envConfig = envConfig,
            )
        }
    }

    private  val updatePipeline: Pipeline by lazy {
        UpdatePipeline(
            id = "update",
            envConfig = envConfig,
            dynamoDbConnector = dynamoDbConnector,
            eventCodec = JsonEventCodec, // NOOP, Should not be required.
            toUpdateRequest = ::toUpdateRequest
        )
    }

    val assembler: PipelineAssembler by lazy {
        PipelineAssembler
            .builder()
            .addPipeline(updatePipeline)
            .build()
    }

    val kinesisAdapter: KinesisAdapter by lazy {
        KinesisAdapter(eventCodec = TracerEventCodec)
    }

}