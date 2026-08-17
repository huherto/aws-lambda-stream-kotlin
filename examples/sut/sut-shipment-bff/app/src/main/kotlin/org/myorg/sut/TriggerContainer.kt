package org.myorg.sut

import io.github.huherto.awsLambdaStream.GlobalRegistry
import io.github.huherto.awsLambdaStream.PipelineAssembler
import io.github.huherto.awsLambdaStream.connectors.DefaultDynamoDbClientFactory
import io.github.huherto.awsLambdaStream.connectors.DynamoDbConnector
import io.github.huherto.awsLambdaStream.flavors.CdcPipeline
import io.github.huherto.awsLambdaStream.flavors.Pipeline
import io.github.huherto.awsLambdaStream.from.DynamodbAdapter
import io.github.huherto.awsLambdaStream.sinks.EventPublisher

class TriggerContainer(
    val dynamoDbConnector: DynamoDbConnector,
    val eventPublisher: EventPublisher,
) {

    companion object {

        fun build() : TriggerContainer {
            val envConfig = GlobalRegistry.envConfig()
            val dynamoDbClientFactory = DefaultDynamoDbClientFactory()
            val dynamoDbConnector = DynamoDbConnector(dynamoDbClientFactory = dynamoDbClientFactory)
            val faultManager = GlobalRegistry.faultManager()

            return TriggerContainer(
                eventPublisher = GlobalRegistry.eventPublisher(),
                dynamoDbConnector = dynamoDbConnector,
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
            .build()
    }

    val dynamoDbAdapter : DynamodbAdapter by lazy {
        DynamodbAdapter()
    }

}