package org.myorg.sut

import software.amazon.awscdk.Aws
import software.amazon.awscdk.Duration
import software.amazon.awscdk.RemovalPolicy
import software.amazon.awscdk.services.dynamodb.*
import software.amazon.awscdk.services.kinesis.Stream
import software.amazon.awscdk.services.lambda.Code
import software.amazon.awscdk.services.lambda.Function
import software.amazon.awscdk.services.lambda.Runtime
import software.amazon.awscdk.services.lambda.StartingPosition
import software.amazon.awscdk.services.lambda.eventsources.DynamoEventSource
import software.amazon.awscdk.services.lambda.eventsources.KinesisEventSource
import software.constructs.Construct

class ControlStack(scope: Construct, serviceProps: ServiceProps) : BaseStack(scope, serviceProps) {

    val tableName = "${service()}-${stage()}-events"
    val trigger = newTriggerLambda()
    val eventsTable = newDynamoDbTable()
    val listener = newListenerLambda()

    init {
        addDynamoDBStreamToLambda(trigger, eventsTable)
        addGSI(eventsTable)
        // addReplicas(eventsTable)
        addKinesisEventSourceToListener(listener)
    }

    private fun deviceIdKey() =
        Attribute.builder().name("id").type(AttributeType.STRING).build()

    private fun newTriggerLambda(): Function =
        Function.Builder.create(this, "trigger")
            .functionName("${subsys()}-control-service-${stage()}-trigger")
            .code(Code.fromAsset("../app/build/libs/sut-control-service.jar"))
            .handler("org.myorg.sut.Trigger::handleRequest")
            .timeout(Duration.seconds(50))
            .memorySize(1024)
            .runtime(Runtime.JAVA_21)
            .environment(mapOf(
                "JAVA_TOOL_OPTIONS" to "-Dslf4j.provider=io.github.vitalijr2.aws.lambda.slf4j.AWSLambdaServiceProvider",
                "EVENT_TABLE_NAME" to tableName,
                "LOG_DEFAULT_LEVEL" to "DEBUG",
            ))
            .build()

    private fun newDynamoDbTable() : TableV2 = TableV2.Builder.create(this, "EventsTable")
        .tableName(tableName)
        .partitionKey(
            Attribute.builder()
                .name("pk")
                .type(AttributeType.STRING)
                .build()
        )
        .sortKey(
            Attribute.builder()
                .name("sk")
                .type(AttributeType.STRING)
                .build()
        )
        .billing(Billing.onDemand())
        .removalPolicy(RemovalPolicy.RETAIN)
        .timeToLiveAttribute("ttl")
        .dynamoStream(StreamViewType.NEW_AND_OLD_IMAGES)
        .build()

    private fun addDynamoDBStreamToLambda(function: Function, table: TableV2) {
        function.addEventSource(
            DynamoEventSource.Builder.create(table)
                .startingPosition(StartingPosition.TRIM_HORIZON)
                .batchSize(5)
                .bisectBatchOnError(true)
                .build()
        )
    }

    private fun addGSI(eventsTable: TableV2) {
        eventsTable.addGlobalSecondaryIndex(GlobalSecondaryIndexPropsV2.builder()
            .indexName("gsi1")
            .partitionKey(Attribute.builder()
                .name("discriminator")
                .type(AttributeType.STRING)
                .build())
            .sortKey(Attribute.builder()
                .name("pk")
                .type(AttributeType.STRING)
                .build())
            .projectionType(ProjectionType.ALL)
            .build())
    }

    private fun addReplicas(eventsTable: TableV2) {
        eventsTable.addReplica(
            ReplicaTableProps.builder()
                .region("us-east-1")
                .build())
    }

    private fun newListenerLambda(): Function =
        Function.Builder.create(this, "listener")
            .functionName("${subsys()}-control-service-${stage()}-listener")
            .code(Code.fromAsset("../app/build/libs/sut-control-service.jar"))
            .handler("org.myorg.sut.Listener::handleRequest")
            .timeout(Duration.seconds(50))
            .memorySize(1024)
            .runtime(Runtime.JAVA_21)
            .environment(mapOf(
                "JAVA_TOOL_OPTIONS" to "-Dslf4j.provider=io.github.vitalijr2.aws.lambda.slf4j.AWSLambdaServiceProvider",
                "EVENT_TABLE_NAME" to tableName,
                "LOG_DEFAULT_LEVEL" to "DEBUG",
            ))
            .build()


    private fun addKinesisEventSourceToListener(listener: Function) {
        val streamName = "${subsys()}-event-hub-${stage()}-s1"
        val accountId = Aws.ACCOUNT_ID
        val regionName = Aws.REGION
        val streamArn = "arn:aws:kinesis:${regionName}:${accountId}:stream/${streamName}"
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