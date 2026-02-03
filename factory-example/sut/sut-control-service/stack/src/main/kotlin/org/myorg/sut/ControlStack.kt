package org.myorg.sut

import software.amazon.awscdk.Aws
import software.amazon.awscdk.Duration
import software.amazon.awscdk.RemovalPolicy
import software.amazon.awscdk.services.dynamodb.Attribute
import software.amazon.awscdk.services.dynamodb.AttributeType
import software.amazon.awscdk.services.dynamodb.StreamViewType
import software.amazon.awscdk.services.dynamodb.Table
import software.amazon.awscdk.services.kinesis.Stream
import software.amazon.awscdk.services.lambda.Code
import software.amazon.awscdk.services.lambda.Function
import software.amazon.awscdk.services.lambda.StartingPosition
import software.amazon.awscdk.services.lambda.eventsources.DynamoEventSource
import software.amazon.awscdk.services.lambda.Runtime
import software.amazon.awscdk.services.lambda.eventsources.KinesisEventSource
import software.constructs.Construct

class ControlStack(scope: Construct, serviceProps: ServiceProps) : BaseStack(scope, serviceProps) {

    val tableName = "${service()}-${stage()}-events"
    val trigger = newTriggerLambda()
    val dynamoDb = newDynamoDbTable()
    val listener = newListenerLambda()

    init {
        addDynamoDBStreamToLambda(trigger, dynamoDb)
        addKinesisEventSourceToListener(listener)
    }

    private fun deviceIdKey() =
        Attribute.builder().name("id").type(AttributeType.STRING).build()

    private fun newTriggerLambda(): Function =
        Function.Builder.create(this, "trigger")
            .functionName("trigger")
            .code(Code.fromAsset("../serverless/build/libs/serverless.jar"))
            .handler("org.myorg.sut.Trigger::handleRequest")
            .timeout(Duration.seconds(50))
            .memorySize(1024)
            .runtime(Runtime.JAVA_21)
            .build()

    private fun newDynamoDbTable(): Table = Table.Builder
        .create(this, tableName)
        .tableName(tableName)
        .partitionKey(deviceIdKey())
        .removalPolicy(RemovalPolicy.DESTROY)
        .stream(StreamViewType.NEW_IMAGE)
        .build()

    private fun addDynamoDBStreamToLambda(function: Function, table: Table) {
        function.addEventSource(
            DynamoEventSource.Builder.create(table)
                .startingPosition(StartingPosition.TRIM_HORIZON)
                .batchSize(5)
                .bisectBatchOnError(true)
                .build()
        )
    }

    private fun newListenerLambda(): Function =
        Function.Builder.create(this, "listener")
            .functionName("sut-control-service-${stage()}-listener")
            .code(Code.fromAsset("../serverless/build/libs/serverless.jar"))
            .handler("org.myorg.sut.Listener::handleRequest")
            .timeout(Duration.seconds(50))
            .memorySize(1024)
            .runtime(Runtime.JAVA_21)
            .environment(mapOf(
                "JAVA_TOOL_OPTIONS" to "-Dslf4j.provider=io.github.vitalijr2.aws.lambda.slf4j.AWSLambdaServiceProvider"))
            .build()


    private fun addKinesisEventSourceToListener(listener: Function) {
        val streamName = "${subsys()}-event-hub-${stage()}-s1"
        val accountId = Aws.ACCOUNT_ID
        val regionName = Aws.REGION
        val streamArn = "arn:aws:kinesis:${regionName}:${accountId}:stream/${streamName}";
        val stream1 = Stream.fromStreamArn(this, "Stream1", streamArn)

        listener.addEventSource(
            KinesisEventSource.Builder.create(stream1)
                .startingPosition(StartingPosition.LATEST)
                .batchSize(100) // up to 10,000; default is 100
                .maxBatchingWindow(Duration.seconds(1)) // up to 5 min
                .bisectBatchOnError(true)
                .retryAttempts(3)
                .parallelizationFactor(1) // up to 10 per shard
                // .onFailure(SqsDlq(dlq))
                // .consumer(consumer) // uncomment if using enhanced fan-out
                // .reportBatchItemFailures(true) // requires special response object in handler
                .build()
        )

    }

}
