package org.myorg.sut

import io.kopipes.aws.AwsEnvironmentConfig
import io.kopipes.aws.AwsKopipesContext
import io.kopipes.aws.connectors.DefaultDynamoDbClientFactory
import io.kopipes.aws.from.KinesisAdapter
import io.kopipes.aws.sinks.DynamoDbEventsMicrostore
import io.kopipes.core.PipelineAssembler
import io.kopipes.core.filters.EventFilter
import io.kopipes.core.flavors.CollectPipeline
import io.kopipes.core.sinks.EventsMicrostore

class ListenerContainer(
    val context: AwsKopipesContext,
    val eventsMicrostore: EventsMicrostore = DynamoDbEventsMicrostore(context),
    val collectPipeline: CollectPipeline = CollectPipeline.builder()
        .id("coll1")
        .eventFilter(EventFilter.Any)
        .eventsMicrostore(eventsMicrostore)
        .build(),
    val assembler: PipelineAssembler = PipelineAssembler.builder()
        .context(context)
        .addPipeline(collectPipeline)
        .build(),
    val kinesisAdapter: KinesisAdapter = KinesisAdapter(context, TrackedUnitEventCodec),
) {
    companion object {
        fun build(): ListenerContainer {
            val envConfig = AwsEnvironmentConfig()
            val context = AwsKopipesContext(
                envConfig = envConfig,
                dynamoDbClientFactory = DynamoDBClientWrapperFactory(DefaultDynamoDbClientFactory(envConfig)),
            )
            return ListenerContainer(context)
        }
    }
}
