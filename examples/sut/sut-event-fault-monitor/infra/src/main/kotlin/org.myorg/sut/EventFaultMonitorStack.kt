package org.myorg.sut

import software.amazon.awscdk.services.lambda.Code
import software.amazon.awscdk.services.lambda.Runtime
import software.constructs.Construct

class EventFaultMonitorStack(scope: Construct, serviceProps: ServiceProps) : BaseStack(scope, serviceProps) {

    val logGroupName = "/aws/kinesisfirehose/${service()}-${stage()}-DeliveryStream"
    val deliveryStreamName = "${service()}-${stage()}-DeliveryStream"
    val busName = "${subsys()}-event-hub-${stage()}-bus"
    val bucketName = "${org()}-${service()}-${stage()}-${regionName()}"
    val jarFile = Code.fromAsset("../app/build/libs/sut-event-fault-monitor.jar")
    val runtime: Runtime = Runtime.JAVA_21!!

    val logGroup = newLogGroup()
    val logStream = newLogStream(logGroup)
    val bucket = newBucket()
    val topic = newTopic()
    val transformLambda = newTransformLambda()
    val deliveryRole = newDeliveryRole(bucket, logGroup, transformLambda)
    val deliveryStream = newDeliveryStream(bucket, deliveryRole, logGroup, logStream, transformLambda)
    val eventBridgeRole = newEventBridgeRole(deliveryStream)

    init {
        newEventRule(deliveryStream, eventBridgeRole)
        grantTopicPublish(topic, transformLambda)
        newBucketOutputs(bucket)
        newTopicOutputs(topic)

        // Enable these after the destination bucket exists in the mirror region.
        // val bucketReplicationRole = newBucketReplicationRole(bucket)
        // newBucketReplicationPolicy(bucket)
    }
}