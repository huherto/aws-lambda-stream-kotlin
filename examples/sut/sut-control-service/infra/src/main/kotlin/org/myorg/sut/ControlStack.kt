package org.myorg.sut

import software.amazon.awscdk.Aws
import software.amazon.awscdk.Duration
import software.amazon.awscdk.RemovalPolicy
import software.amazon.awscdk.services.dynamodb.*
import software.amazon.awscdk.services.kinesis.IStream
import software.amazon.awscdk.services.kinesis.Stream
import software.amazon.awscdk.services.lambda.Code
import software.amazon.awscdk.services.lambda.Function
import software.amazon.awscdk.services.lambda.Runtime
import software.amazon.awscdk.services.lambda.StartingPosition
import software.amazon.awscdk.services.lambda.eventsources.DynamoEventSource
import software.amazon.awscdk.services.lambda.eventsources.KinesisEventSource
import software.constructs.Construct

class ControlStack(scope: Construct, serviceProps: ServiceProps) : BaseStack(scope, serviceProps) {

    internal val eventsTable = newEventsTable()
    internal val stream1 = newStream1()

    val JarFile = Code.fromAsset("../app/build/libs/sut-control-service.jar")
    val runtime: Runtime = Runtime.JAVA_21

    init {
        // Events Table -> Dynamo DB Stream -> Trigger Lambda
        val trigger = newTriggerLambda()
        consumeFromTable(trigger, eventsTable)
        addGSI(eventsTable)
        // addReplicas(eventsTable)

        // Kinesis Stream -> Listener Lambda
        val listener = newListenerLambda()
        consumeFromKinesis(listener, stream1)
    }

}

fun ControlStack.eventsTableName() = "${service()}-${stage()}-events"
fun ControlStack.busName() = "${subsys()}-event-hub-${stage()}-bus"

fun ControlStack.newTriggerLambda(): Function =
    Function.Builder.create(this, "trigger")
        .functionName("${subsys()}-control-service-${stage()}-trigger")
        .code(JarFile)
        .handler("org.myorg.sut.Trigger::handleRequest")
        .timeout(Duration.seconds(50))
        .memorySize(1024)
        .runtime(runtime)
        .environment(mapOf(
            "JAVA_TOOL_OPTIONS" to "-Dslf4j.provider=io.github.vitalijr2.aws.lambda.slf4j.AWSLambdaServiceProvider",
            "EVENT_TABLE_NAME" to eventsTableName(),
            "BUS_NAME" to busName(),
            "BUS_SRC" to "control-service-trigger",
            "LOG_DEFAULT_LEVEL" to "DEBUG",
        ))
        .build()

fun ControlStack.newEventsTable() : TableV2 = TableV2.Builder.create(this, "EventsTable")
    .tableName(eventsTableName())
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

fun ControlStack.consumeFromTable(function: Function, table: TableV2) {
    function.addEventSource(
        DynamoEventSource.Builder.create(table)
            .startingPosition(StartingPosition.TRIM_HORIZON)
            .batchSize(5)
            .bisectBatchOnError(true)
            .build()
    )
}

fun ControlStack.addGSI(eventsTable: TableV2) {
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

fun ControlStack.addReplicas(eventsTable: TableV2) {
    eventsTable.addReplica(
        ReplicaTableProps.builder()
            .region("us-east-1")
            .build())
}

fun ControlStack.newListenerLambda(): Function =
    Function.Builder.create(this, "listener")
        .functionName("${subsys()}-control-service-${stage()}-listener")
        .code(JarFile)
        .handler("org.myorg.sut.Listener::handleRequest")
        .timeout(Duration.seconds(50))
        .memorySize(1024)
        .runtime(runtime)
        .environment(mapOf(
            "JAVA_TOOL_OPTIONS" to "-Dslf4j.provider=io.github.vitalijr2.aws.lambda.slf4j.AWSLambdaServiceProvider",
            "EVENT_TABLE_NAME" to eventsTableName(),
            "BUS_NAME" to busName(),
            "BUS_SRC" to "control-service-listener",
            "LOG_DEFAULT_LEVEL" to "DEBUG",
        ))
        .build()


fun ControlStack.newStream1(): IStream {
    val streamName = "${subsys()}-event-hub-${stage()}-s1"
    val accountId = Aws.ACCOUNT_ID
    val regionName = Aws.REGION
    val streamArn = "arn:aws:kinesis:${regionName}:${accountId}:stream/${streamName}"
    return Stream.fromStreamArn(this, "Stream1", streamArn)
}

fun ControlStack.consumeFromKinesis(listener: Function, stream: IStream) {
    listener.addEventSource(
        KinesisEventSource.Builder.create(stream)
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