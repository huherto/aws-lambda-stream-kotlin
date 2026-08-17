package org.myorg.sut

import io.github.huherto.awsLambdaStream.PipelineAssembler
import io.github.huherto.awsLambdaStream.connectors.DefaultS3ClientFactory
import io.github.huherto.awsLambdaStream.connectors.S3Connector
import io.github.huherto.awsLambdaStream.flavors.MaterializeS3Pipeline
import io.github.huherto.awsLambdaStream.flavors.Pipeline
import io.github.huherto.awsLambdaStream.from.DynamodbAdapter

class DynamoDbTriggerContainer (
    val s3Connector: S3Connector,
) {

    companion object {

        fun build() : DynamoDbTriggerContainer {
            val defaultS3ClientFactory = DefaultS3ClientFactory()
            val s3Connector = S3Connector(defaultS3ClientFactory)

            return DynamoDbTriggerContainer(
                s3Connector = s3Connector,
            )
        }
    }

    private  val materializeS3Pipeline: Pipeline by lazy {
        MaterializeS3Pipeline(
            pipelineId = "t1",
            s3Connector = s3Connector,
            toPutRequest = ::toS3PutRequest
        )
    }

    val assembler: PipelineAssembler by lazy {
        PipelineAssembler
            .builder()
            .addPipeline(materializeS3Pipeline)
            .build()
    }

    val dynamoDbAdapter:  DynamodbAdapter by lazy { DynamodbAdapter() }

}