package org.myorg.sut

import io.kopipes.aws.AwsKopipesContext
import io.kopipes.aws.flavors.MaterializeS3Pipeline
import io.kopipes.aws.from.DynamodbAdapter
import io.kopipes.core.PipelineAssembler
import io.kopipes.core.flavors.Pipeline

class DynamoDbTriggerContainer(val context: AwsKopipesContext) {

    companion object {

        fun build() : DynamoDbTriggerContainer {
            return DynamoDbTriggerContainer(AwsKopipesContext())
        }
    }

    private  val materializeS3Pipeline: Pipeline by lazy {
        MaterializeS3Pipeline.builder()
            .id("t1")
            .context(context)
            .toPutRequest(::toS3PutRequest)
            .build()
    }

    val assembler: PipelineAssembler by lazy {
        PipelineAssembler
            .builder()
            .context(context)
            .addPipeline(materializeS3Pipeline)
            .build()
    }

    val dynamoDbAdapter:  DynamodbAdapter by lazy { DynamodbAdapter(context) }

}
