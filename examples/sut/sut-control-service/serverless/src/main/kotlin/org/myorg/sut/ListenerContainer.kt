package org.myorg.sut

import io.github.huherto.awsLambdaStream.EnvironmentConfig
import io.github.huherto.awsLambdaStream.FaultManager
import io.github.huherto.awsLambdaStream.PipelineAssembler
import io.github.huherto.awsLambdaStream.flavors.CollectPipeline
import io.github.huherto.awsLambdaStream.flavors.Pipeline
import io.github.huherto.awsLambdaStream.from.KinesisAdapter
import io.github.huherto.awsLambdaStream.getDynamoDbClient
import io.github.huherto.awsLambdaStream.sinks.EventBridgePublishOptions
import io.github.huherto.awsLambdaStream.sinks.EventBridgePublisher
import io.github.huherto.awsLambdaStream.sinks.EventsMicrostore
import io.github.huherto.awsLambdaStream.sinks.EventsMicrostoreImpl
import java.nio.ByteBuffer

class ListenerContainer(
    val envConfig: EnvironmentConfig,
    val eventsMicrostore: EventsMicrostore,
    val faultManager: FaultManager,
) {

    companion object {
        fun build() : ListenerContainer {
            val envConfig = EnvironmentConfig()
            val dynamoDbClient = DynamoDBClientWrapper(getDynamoDbClient(envConfig))
            val eventPublisherOptions = EventBridgePublishOptions(envConfig)
            val eventPublisher = EventBridgePublisher(eventPublisherOptions)
            val faultManager = FaultManager(envConfig, eventPublisher)
            val eventsMicrostore = EventsMicrostoreImpl(
                envConfig = envConfig,
                dynamoDbClient = dynamoDbClient,
                faultManager = faultManager
            )
            return ListenerContainer(
                envConfig = envConfig,
                eventsMicrostore = eventsMicrostore,
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

    private val collectPipeline: Pipeline by lazy {
        CollectPipeline(
            pipelineId = "collect1",
            envConfig = envConfig,
            eventsMicrostore = eventsMicrostore,
            onEventClass = listOf(TrackedUnitEvent::class)
        )
    }

    val assembler: PipelineAssembler by lazy {
        PipelineAssembler.Companion
            .builder()
            .faultManager(faultManager)
            .addPipeline(collectPipeline)
            .build()
    }

}