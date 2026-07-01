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

Resp API -> Dynamo DB Table -> Dynamo DB Stream -> Dynamo DB Trigger Lambda ->  S3 Bucket ?

S3 bucket -> SNS Topic -> SQS Trigger Queue -> S3 Trigger Lambda -> Event Bus -> Kinesis Stream 1

Kinesis Stream 1 -> Dynamo DB Table.

*/

class RegionalHealthCheckStack(scope: Construct, serviceProps: ServiceProps) : BaseStack(scope, serviceProps) {

    val JarFile = Code.fromAsset("../app/build/libs/sut-regional-health-check.jar")
    val runtime: Runtime = Runtime.JAVA_21!!
    val runtimeEnvironment = mapOf(
        "JAVA_TOOL_OPTIONS" to "-Dslf4j.provider=io.github.vitalijr2.aws.lambda.slf4j.AWSLambdaServiceProvider",
        "LOG_DEFAULT_LEVEL" to "DEBUG",
        "ENTITY_TABLE_NAME" to entityTableName(),
//        "BUS_NAME" to "${service()}-${stage()}-bus",
//        "BUS_SRC" to "regional-health-check-trigger",
        "REGION" to regionName(),
//        "ACCOUNT_ID" to Aws.ACCOUNT_ID,
//        "BUCKET_NAME" to "${org()}-${service()}-${stage()}-${regionName()}"
    )
    internal val topic: Topic = newTopic()
    internal val triggerQueue: Queue = newTriggerQueue()
    internal val bucket: Bucket = newBucket(topic)
    internal val entitiesTable = newEntitiesTable()
    internal val bus: EventBus = newBus()
    internal val stream1: CfnStream = newStream1()
    internal val disabled = false

    init {

        val restApiLambda = newRestApiLambda()
        addEntitiesTablePermissions(restApiLambda, entitiesTable)

        if (disabled) {
            newTopicOutputs(topic)
            newBucketOutputs(bucket)
            newTriggerQueueOutputs(triggerQueue)

            // From S3 Bucket to SNS Topic
            allowBucketPublishToTopic(topic, bucket)

            // From SNS Topic to SQS Queue
            allowTopicToSendMessagesToQueue(
                triggerQueue = triggerQueue,
                topic = topic,
            )
            subscribeToTopic(
                triggerQueue = triggerQueue,
                topic = topic,
            )

            // From SQS Queue to S3Trigger Lambda to Event Bus
            val triggerLambda = newTriggerLambda()
            addSqsEventSourceToTrigger(triggerLambda, triggerQueue)

            // From Event Bus to Logs and Kinesis Stream 1
            logBusEventsInCloudWatch(bus)
            val kinesisBusRole = newKinesisBusRole(stream1)
            addStream1EventRule(
                bus = bus,
                stream1 = stream1,
                busRole = kinesisBusRole,
            )

            addApiGatewayApiKeys()
            createRegionalHealthCheck()
            createSyntheticsCanary(
                bucket = bucket,
                healthCheckEndpoint = healthCheckEndpoint(),
                apiKey = apiKey(),
            )

            // placeholder
            val myFunction = Function.Builder.create(this, "Function").build()

            // Example once a Lambda consuming/writing the table exists:

            addEntitiesTableStreamToLambda(myFunction, entitiesTable)
            addBusPutEventsPermissions(myFunction, bus)
            addKmsPermissions(myFunction)
        }
    }

    private fun newTriggerLambda(): Function =
        Function.Builder.create(this, "trigger")
            .functionName("${subsys()}-regional-health-check-${stage()}-trigger")
            .code(JarFile)
            .handler("org.myorg.sut.S3Trigger::handleRequest")
            .timeout(Duration.seconds(50))
            .memorySize(1024)
            .runtime(runtime)
            .environment(runtimeEnvironment)
            .build()

    private fun newRestApiLambda(): Function =
        Function.Builder.create(this, "checkHealthApi")
            .functionName("${subsys()}-regional-health-check-${stage()}-checkHealthApi")
            .code(JarFile)
            .handler("org.myorg.sut.CheckHealthApi::handleRequest")
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