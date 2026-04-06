package org.myorg.sut

//private fun deviceIdKey() =
//    Attribute.builder().name("id").type(AttributeType.STRING).build()
//
//private fun MyStack.newDynamoDbTable(): Table = Table.Builder
//    .create(this, "my-table")
//    .tableName("my-table")
//    .partitionKey(deviceIdKey())
//    .removalPolicy(RemovalPolicy.DESTROY)
//    .stream(StreamViewType.NEW_IMAGE)
//    .build()
//
//
//private fun MyStack.addDynamoDBStreamToLambda(function: Function, table: Table) {
//    function.addEventSource(
//        DynamoEventSource.Builder.create(table)
//            .startingPosition(StartingPosition.TRIM_HORIZON)
//            .batchSize(5)
//            .bisectBatchOnError(true)
//            .build()
//    )
//}