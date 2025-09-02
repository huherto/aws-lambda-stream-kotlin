package org.myorg.sut

import software.amazon.awscdk.Duration
import software.amazon.awscdk.RemovalPolicy
import software.amazon.awscdk.services.dynamodb.Attribute
import software.amazon.awscdk.services.dynamodb.AttributeType
import software.amazon.awscdk.services.dynamodb.StreamViewType
import software.amazon.awscdk.services.dynamodb.Table
import software.amazon.awscdk.services.lambda.Code
import software.amazon.awscdk.services.lambda.Function
import software.amazon.awscdk.services.lambda.StartingPosition
import software.amazon.awscdk.services.lambda.eventsources.DynamoEventSource
import software.amazon.awscdk.services.lambda.Runtime
import software.constructs.Construct

class ControlStack(scope: Construct, serviceProps: ServiceProps) : BaseStack(scope, serviceProps) {

    val tableName = "${service()}-${stage()}-events"
    val listener = newListenerLambda()
    val dynamoDb = newDynamoDbTable()

    init {
        addDynamoDBStreamToLambda(newListenerLambda(), newDynamoDbTable())
    }

    private fun deviceIdKey() =
        Attribute.builder().name("id").type(AttributeType.STRING).build()

    private fun newListenerLambda(): Function =
        Function.Builder.create(this, "listener")
            .functionName("listener")
            .code(Code.fromAsset("../serverless/build/libs/serverless.jar"))
            .handler("org.myorg.sut.Listener::handleRequest")
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

}
