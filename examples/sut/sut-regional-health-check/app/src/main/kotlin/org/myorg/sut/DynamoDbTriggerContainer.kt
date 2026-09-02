package org.myorg.sut

import io.github.huherto.awsLambdaStream.PipelineAssembler
import io.github.huherto.awsLambdaStream.flavors.MaterializeS3Pipeline
import io.github.huherto.awsLambdaStream.flavors.Pipeline
import io.github.huherto.awsLambdaStream.from.DynamodbAdapter

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