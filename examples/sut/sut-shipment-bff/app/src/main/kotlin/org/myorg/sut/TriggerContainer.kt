package org.myorg.sut

import io.github.huherto.awsLambdaStream.EnvironmentConfig
import io.github.huherto.awsLambdaStream.FaultManager
import io.github.huherto.awsLambdaStream.PipelineAssembler
import io.github.huherto.awsLambdaStream.connectors.DefaultDynamoDbClientFactory
import io.github.huherto.awsLambdaStream.connectors.DynamoDbConnector
import io.github.huherto.awsLambdaStream.flavors.CdcPipeline
import io.github.huherto.awsLambdaStream.flavors.Pipeline
import io.github.huherto.awsLambdaStream.from.DynamodbAdapter
import io.github.huherto.awsLambdaStream.sinks.EventBridgePublisher
import io.github.huherto.awsLambdaStream.sinks.EventPublisher
import mu.KotlinLogging.logger

class TriggerContainer(
    val dynamoDbConnector: DynamoDbConnector,
    val eventPublisher: EventPublisher,
    val faultManager: FaultManager,
) {

    companion object {

        private val logger = logger {}

        fun build() : TriggerContainer {
            val envConfig = EnvironmentConfig()
            val dynamoDbClientFactory = DefaultDynamoDbClientFactory(envConfig)
            val dynamoDbConnector = DynamoDbConnector(clientFactory = dynamoDbClientFactory)
            val eventPublisher = EventBridgePublisher(envConfig)
            val faultManager = FaultManager(envConfig, eventPublisher)

            return TriggerContainer(
                eventPublisher = eventPublisher,
                dynamoDbConnector = dynamoDbConnector,
                faultManager = faultManager,
            )
        }
    }

    private val cdcPipeline: Pipeline by lazy {
        CdcPipeline(
            id = "cdc1",
            dynamoDbConnector = dynamoDbConnector,
            eventPublisher = eventPublisher,
            toEvent = ::toEvent,
        )
    }

    val assembler: PipelineAssembler by lazy {
        PipelineAssembler
            .builder()
            .addPipeline(cdcPipeline)
            .faultManager(faultManager)
            .build()
    }

    val dynamoDbAdapter = DynamodbAdapter(faultManager)

}