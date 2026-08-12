package org.myorg.sut

import software.amazon.awscdk.Duration
import software.amazon.awscdk.services.lambda.Code
import software.amazon.awscdk.services.lambda.Function
import software.amazon.awscdk.services.lambda.Runtime
import software.amazon.awscdk.services.logs.LogGroup
import software.amazon.awscdk.services.logs.LogStream
import software.amazon.awscdk.services.s3.Bucket
import software.amazon.awscdk.services.sns.Topic
import software.amazon.awscdk.services.sqs.Queue
import software.constructs.Construct

class EventFaultMonitorStack(scope: Construct, serviceProps: ServiceProps) : BaseStack(scope, serviceProps) {

    val logGroupName = "/aws/kinesisfirehose/${service()}-${stage()}-DeliveryStream"
    val deliveryStreamName = "${service()}-${stage()}-DeliveryStream"
    val busName = "${subsys()}-event-hub-${stage()}-bus"
    val bucketName = "${org()}-${service()}-${stage()}-${regionName()}"

    internal val logGroup: LogGroup = newLogGroup()
    internal val logStream: LogStream = newLogStream(logGroup)
    internal val bucket: Bucket = newBucket()
    internal val topic: Topic = newTopic()
    internal val notificationVerificationQueue: Queue = newNotificationVerificationQueue()

    val JarFile = Code.fromAsset("../app/build/libs/sut-event-fault-monitor.jar")
    val runtime: Runtime = Runtime.JAVA_21!!
    val runtimeEnvironment = mapOf(
        "JAVA_TOOL_OPTIONS" to "-Dslf4j.provider=io.github.vitalijr2.aws.lambda.slf4j.AWSLambdaServiceProvider",
        "LOG_DEFAULT_LEVEL" to "DEBUG",
        "TOPIC_ARN" to topic.topicArn,
    )

    init {
        // Event Bridge -> Firehose -> Transform Lambda
        val transformLambda = newTransformLambda()
        val deliveryStream = newDeliveryStream(bucket, logGroup, logStream, transformLambda)
        val eventBridgeRole = newEventBridgeRole(deliveryStream)
        publishToFirehose(deliveryStream, eventBridgeRole)

        // Transform Lambda -> Topic
        grantAccessToTopic(transformLambda, topic)

        // Topic -> Notification Verification Queue
        publishToQueue(topic, notificationVerificationQueue)

        // Enable these after the destination bucket exists in the mirror region.
        // val bucketReplicationRole = newBucketReplicationRole(bucket)
        // newBucketReplicationPolicy(bucket)
    }

    private fun newTransformLambda(): Function =
        Function.Builder.create(this, "transformLambda")
            .functionName("${service()}-${stage()}-transform")
            .code(JarFile)
            .handler("org.myorg.sut.Transform::handleRequest")
            .timeout(Duration.seconds(60))
            .memorySize(1024)
            .runtime(runtime)
            .environment(runtimeEnvironment)
            .build()
}