package org.myorg.sut

import io.github.huherto.awsLambdaStream.EnvironmentConfig
import io.github.huherto.awsLambdaStream.GlobalRegistry
import io.github.huherto.awsLambdaStream.PipelineAssembler
import io.github.huherto.awsLambdaStream.connectors.DefaultS3ClientFactory
import io.github.huherto.awsLambdaStream.connectors.S3Connector
import io.github.huherto.awsLambdaStream.filters.EventFilter
import io.github.huherto.awsLambdaStream.flavors.CdcPipeline
import io.github.huherto.awsLambdaStream.flavors.Pipeline
import io.github.huherto.awsLambdaStream.from.S3Adapter
import io.github.huherto.awsLambdaStream.sinks.EventPublisher

class S3TriggerContainer(
    val envConfig: EnvironmentConfig,
    val s3Connector: S3Connector,
    val eventPublisher: EventPublisher,
) {

    companion object {

        fun build() : S3TriggerContainer {
            val envConfig = GlobalRegistry.envConfig()
            val s3ClientFactory = DefaultS3ClientFactory(envConfig)
            val s3Connector = S3Connector(clientFactory = s3ClientFactory)

            return S3TriggerContainer(
                envConfig = envConfig,
                eventPublisher = GlobalRegistry.eventPublisher(),
                s3Connector = s3Connector,
            )
        }
    }

    private val cdcPipeline: Pipeline by lazy {
        CdcPipeline(
            id = "cdc",
            envConfig = envConfig,
            eventPublisher = eventPublisher,
            eventFilter = EventFilter.Any
        )
    }

    val assembler: PipelineAssembler by lazy {
        PipelineAssembler
            .builder()
            .addPipeline(cdcPipeline)
            .build()
    }

    val s3Adapter : S3Adapter by lazy {
        S3Adapter(
            s3Connector = s3Connector,
            eventCodec = TracerEventCodec,
        )
    }

}