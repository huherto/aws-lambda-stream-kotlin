package org.myorg.sut

import io.kopipes.aws.PipelineAssembler
import io.kopipes.aws.flavors.MaterializeS3Pipeline
import io.kopipes.aws.flavors.Pipeline
import io.kopipes.aws.from.DynamodbAdapter

class DynamoDbTriggerContainer() {

    companion object {

        fun build() : DynamoDbTriggerContainer {
            return DynamoDbTriggerContainer()
        }
    }

    private  val materializeS3Pipeline: Pipeline by lazy {
        MaterializeS3Pipeline.builder()
            .id("t1")
            .toPutRequest(::toS3PutRequest)
            .build()
    }

    val assembler: PipelineAssembler by lazy {
        PipelineAssembler
            .builder()
            .addPipeline(materializeS3Pipeline)
            .build()
    }

    val dynamoDbAdapter:  DynamodbAdapter by lazy { DynamodbAdapter() }

}