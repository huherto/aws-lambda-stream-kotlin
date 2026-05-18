package org.myorg.sut

import io.github.huherto.awsLambdaStream.EnvironmentConfig
import io.github.huherto.awsLambdaStream.FaultManager
import io.github.huherto.awsLambdaStream.PipelineAssembler
import io.github.huherto.awsLambdaStream.connectors.DefaultDynamoDbClientFactory
import io.github.huherto.awsLambdaStream.connectors.DynamoDbConnector
import io.github.huherto.awsLambdaStream.filters.EventFilters
import io.github.huherto.awsLambdaStream.flavors.CdcPipeline
import io.github.huherto.awsLambdaStream.flavors.Pipeline
import io.github.huherto.awsLambdaStream.from.DynamodbAdapter
import io.github.huherto.awsLambdaStream.sinks.EventBridgePublishOptions
import io.github.huherto.awsLambdaStream.sinks.EventBridgePublisher
import io.github.huherto.awsLambdaStream.sinks.EventPublisher
import mu.KotlinLogging.logger

class TriggerContainer(
    val envConfig: EnvironmentConfig,
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
            val eventPublisherOptions = EventBridgePublishOptions(envConfig)
            val eventPublisher = EventBridgePublisher(eventPublisherOptions)
            val faultManager = FaultManager(envConfig, eventPublisher)

            return TriggerContainer(
                envConfig = envConfig,
                eventPublisher = eventPublisher,
                dynamoDbConnector = dynamoDbConnector,
                faultManager = faultManager,
            )
        }
    }

    private val cdcPipeline: Pipeline by lazy {
        CdcPipeline(
            id = "cdc1",
            eventFilter = EventFilters.classes(TrackedUnitEvent::class),
            dynamoDbConnector = dynamoDbConnector,
            eventPublisher = eventPublisher,
            //onContentType = TODO(),
            //compactRule = TODO(),
            queryRule = TODO(),
            //queryRelated = TODO(),
            //toQueryRequest = TODO(),
            //toGetRequest = TODO(),
            //toEvent = TODO(),
            //encryptEvent = TODO(),
            //parallel = TODO(),
        )
    }

    val assembler: PipelineAssembler by lazy {
        PipelineAssembler
            .builder()
            .faultManager(faultManager)
            .build()
    }

    val dynamoDbAdapter = DynamodbAdapter(faultManager)

}