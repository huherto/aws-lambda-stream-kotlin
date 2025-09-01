package org.myorg.sut

import software.amazon.awscdk.Duration
import software.amazon.awscdk.RemovalPolicy
import software.amazon.awscdk.services.dynamodb.Attribute
import software.amazon.awscdk.services.dynamodb.AttributeType
import software.amazon.awscdk.services.dynamodb.StreamViewType
import software.amazon.awscdk.services.dynamodb.Table
import software.amazon.awscdk.services.lambda.Code
import software.amazon.awscdk.services.lambda.Function
import software.amazon.awscdk.services.lambda.Runtime
import software.amazon.awscdk.services.lambda.StartingPosition
import software.amazon.awscdk.services.lambda.eventsources.DynamoEventSource

private fun deviceIdKey() =
    Attribute.builder().name("id").type(AttributeType.STRING).build()

private fun MyStack.newDynamoDbTable(): Table = Table.Builder
    .create(this, "my-table")
    .tableName("my-table")
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