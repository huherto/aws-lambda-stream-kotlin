package org.myorg.sut

import software.amazon.awscdk.Duration
import software.amazon.awscdk.services.events.Archive
import software.amazon.awscdk.services.events.EventBus
import software.amazon.awscdk.services.events.EventPattern
import software.amazon.awscdk.services.events.Match
import software.constructs.Construct

class MyStack(scope: Construct, serviceProps: ServiceProps) : BaseStack(scope, serviceProps) {

    val tableName := "${service()}-${stage()}-events"
    val listener: Function = newListenerLambda()
    val dynamoDb: Function = newDynamoDbTable()

    init {

    }

    private fun deviceIdKey() =
        Attribute.builder().name("id").type(AttributeType.STRING).build()

    private fun newListenerLambda(): software.amazon.awscdk.services.lambda.Function =
        Function.Builder.create(this, "listener")
            .functionName("listener")
            .code(Code.fromAsset("../serverless/build/libs/serverless.jar"))
            .handler("org.myorg.example.Listener::handleRequest")
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

    private fun MyStack.addDynamoDBStreamToLambda(function: Function, table: Table) {
        function.addEventSource(
            DynamoEventSource.Builder.create(table)
                .startingPosition(StartingPosition.TRIM_HORIZON)
                .batchSize(5)
                .bisectBatchOnError(true)
                .build()
        )

}
