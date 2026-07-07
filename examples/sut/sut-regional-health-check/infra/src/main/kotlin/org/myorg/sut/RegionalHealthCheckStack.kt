package org.myorg.sut

import software.amazon.awscdk.CfnCondition
import software.amazon.awscdk.Duration
import software.amazon.awscdk.Fn
import software.amazon.awscdk.services.apigateway.CfnApiKey
import software.amazon.awscdk.services.events.EventBus
import software.amazon.awscdk.services.kinesis.CfnStream
import software.amazon.awscdk.services.lambda.Code
import software.amazon.awscdk.services.lambda.Function
import software.amazon.awscdk.services.lambda.Runtime
import software.amazon.awscdk.services.s3.Bucket
import software.amazon.awscdk.services.sns.Topic
import software.amazon.awscdk.services.sqs.Queue
import software.constructs.Construct

/*
Tracer flow diagram:

Check Health API
    -> Dynamo DB Table
        -> Dynamo DB Stream
            -> Dynamo DB Trigger Lambda
                -> S3 bucket
                    -> SNS Topic
                        -> SQS Trigger Queue
                            -> S3 Trigger Lambda
                                -> Event Bus
                                    -> Kinesis Stream 1
                                        -> Kinesis Trigger Lambda
                                            -> Dynamo DB Table.

*/

class RegionalHealthCheckStack(scope: Construct, serviceProps: ServiceProps) : BaseStack(scope, serviceProps) {

    internal val topic: Topic = newTopic()
    internal val triggerQueue: Queue = newTriggerQueue()
    internal val bucket: Bucket = newBucket(topic)
    internal val entitiesTable = newEntitiesTable()
    internal val bus: EventBus = newBus()
    internal val stream1: CfnStream = newStream1()
    internal val disabled = false

    val JarFile = Code.fromAsset("../app/build/libs/sut-regional-health-check.jar")
    val runtime: Runtime = Runtime.JAVA_21!!
    val runtimeEnvironment = mapOf(
        "JAVA_TOOL_OPTIONS" to "-Dslf4j.provider=io.github.vitalijr2.aws.lambda.slf4j.AWSLambdaServiceProvider",
        "LOG_DEFAULT_LEVEL" to "DEBUG",
        "ENTITY_TABLE_NAME" to entitiesTable.tableName,
//        "BUS_NAME" to "${service()}-${stage()}-bus",
//        "BUS_SRC" to "regional-health-check-trigger",
        "REGION" to regionName(),
//        "ACCOUNT_ID" to Aws.ACCOUNT_ID,
        "BUCKET_NAME" to bucket.bucketName,
    )

    init {

        // Check Health API  -> Dynamo DB Table
        val restApiLambda = newCheckHealthApi()
        allowWriteAccessToTable(restApiLambda, entitiesTable)

        // Dynamo DB Table -> Dynamo DB Stream -> Dynamo DB Trigger Lambda -> S3 bucket
        val dynamoDbTriggerLambda = newDynamoDbTriggerLambda()
        configureLambdaEventSource(dynamoDbTriggerLambda, entitiesTable)
        allowWriteAccessToBucket(dynamoDbTriggerLambda, bucket)

        // S3 Bucket -> SNS Topic
        allowBucketPublishToTopic(topic, bucket)

        // SNS Topic -> SQS Trigger Queue
        subscribeToTopic(
            triggerQueue = triggerQueue,
            topic = topic,
        )

        // SQS Trigger Queue -> S3Trigger Lambda -> Event Bus
        val triggerLambda = newS3TriggerLambda()
        addSqsEventSourceToTrigger(triggerLambda, triggerQueue)
        addBusPutEventsPermissions(triggerLambda, bus)

        // Event Bus -> Kinesis Stream 1
        val kinesisBusRole = newKinesisBusRole(stream1)
        addStream1EventRule(
            bus = bus,
            stream1 = stream1,
            busRole = kinesisBusRole,
        )

        // Event Bus -> CloudWatch Log Groups
        logBusEventsInCloudWatch(bus)

        if (disabled) {
            newTopicOutputs(topic)
            newBucketOutputs(bucket)
            newTriggerQueueOutputs(triggerQueue)

            addApiGatewayApiKeys()
            createRegionalHealthCheck()
            createSyntheticsCanary(
                bucket = bucket,
                healthCheckEndpoint = healthCheckEndpoint(),
                apiKey = apiKey(),
            )

            addKmsPermissions(triggerLambda)
        }
    }

    private fun newCheckHealthApi(): Function =
        Function.Builder.create(this, "checkHealthApi")
            .functionName("${subsys()}-regional-health-check-${stage()}-checkHealthApi")
            .code(JarFile)
            .handler("org.myorg.sut.CheckHealthApi::handleRequest")
            .timeout(Duration.seconds(50))
            .memorySize(1024)
            .runtime(runtime)
            .environment(runtimeEnvironment)
            .build()

    private fun newDynamoDbTriggerLambda(): Function =
        Function.Builder.create(this, "dynamoDbTrigger")
            .functionName("${subsys()}-regional-health-check-${stage()}-dynamoDbTrigger")
            .code(JarFile)
            .handler("org.myorg.sut.DynamoDbTrigger::handleRequest")
            .timeout(Duration.seconds(50))
            .memorySize(1024)
            .runtime(runtime)
            .environment(runtimeEnvironment)
            .build()

    private fun newS3TriggerLambda(): Function =
        Function.Builder.create(this, "s3Trigger")
            .functionName("${subsys()}-regional-health-check-${stage()}-s3trigger")
            .code(JarFile)
            .handler("org.myorg.sut.S3Trigger::handleRequest")
            .timeout(Duration.seconds(50))
            .memorySize(1024)
            .runtime(runtime)
            .environment(runtimeEnvironment)
            .build()

    fun RegionalHealthCheckStack.isWestCondition(): CfnCondition =
        CfnCondition.Builder.create(this, "IsWest")
            .expression(Fn.conditionEquals(regionName(), "us-west-2"))
            .build()

    fun RegionalHealthCheckStack.isEastCondition(): CfnCondition =
        CfnCondition.Builder.create(this, "IsEast")
            .expression(Fn.conditionEquals(regionName(), "us-east-1"))
            .build()

    fun RegionalHealthCheckStack.addApiGatewayApiKeys(): List<CfnApiKey> =
        apiKeys().mapIndexed { index, apiKey ->
            CfnApiKey.Builder.create(this, "ApiGatewayApiKey${index + 1}")
                .name(apiKey.name)
                .value(apiKey.value)
                .enabled(true)
                .build()
        }
}