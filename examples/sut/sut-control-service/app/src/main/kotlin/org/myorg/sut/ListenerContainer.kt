package org.myorg.sut

import io.github.huherto.awsLambdaStream.EnvironmentConfig
import io.github.huherto.awsLambdaStream.FaultManager
import io.github.huherto.awsLambdaStream.GlobalRegistry
import io.github.huherto.awsLambdaStream.PipelineAssembler
import io.github.huherto.awsLambdaStream.connectors.DefaultDynamoDbClientFactory
import io.github.huherto.awsLambdaStream.filters.EventFilters
import io.github.huherto.awsLambdaStream.flavors.CollectPipeline
import io.github.huherto.awsLambdaStream.flavors.Pipeline
import io.github.huherto.awsLambdaStream.from.KinesisAdapter
import io.github.huherto.awsLambdaStream.sinks.EventsMicrostore
import io.github.huherto.awsLambdaStream.sinks.EventsMicrostoreImpl

class ListenerContainer(
    val envConfig: EnvironmentConfig,
    val eventsMicrostore: EventsMicrostore,
    val faultManager: FaultManager,
) {

    companion object {
        fun build() : ListenerContainer {
            val envConfig = GlobalRegistry.envConfig()
            val faultManager = GlobalRegistry.faultManager()
            val dynamoDbClientFactory = DynamoDBClientWrapperFactory(DefaultDynamoDbClientFactory(envConfig))
            val eventsMicrostore = EventsMicrostoreImpl(
                envConfig = envConfig,
                dynamoDbClientFactory = dynamoDbClientFactory,
                faultManager = faultManager
            )
            return ListenerContainer(
                envConfig = envConfig,
                eventsMicrostore = eventsMicrostore,
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

    private val collectPipeline: Pipeline by lazy {
        CollectPipeline(
            pipelineId = "coll1",
            envConfig = envConfig,
            eventsMicrostore = eventsMicrostore,
            eventFilter = EventFilters.classes(TrackedUnitEvent::class)
        )
    }

    val assembler: PipelineAssembler by lazy {
        PipelineAssembler
            .builder()
            .faultManager(faultManager)
            .addPipeline(collectPipeline)
            .build()
    }

}