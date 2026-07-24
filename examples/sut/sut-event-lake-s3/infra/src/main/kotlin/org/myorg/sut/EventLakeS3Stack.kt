package org.myorg.sut

import software.constructs.Construct

class EventLakeS3Stack(scope: Construct, serviceProps: ServiceProps) : BaseStack(scope, serviceProps) {

    val replicateBucket = false

    init {
        val bucket = newBucketWithLifecycle()

        configureBucketLogging(bucket)

        if (replicateBucket) {
            // One of these lines seem to cause some sort recursivity error.
            val replicationRole = newBucketReplicationRole(bucket)
            newBucketReplicationPolicy(bucket)
            configureBucketReplication(bucket, replicationRole)
        }

        val logGroup = newFirehoseLogGroup()
        val logStream = newFirehoseLogStream(logGroup)
        val deliveryRole = newDeliveryRole(bucket, logGroup)
        val deliveryStream = newDeliveryStream(bucket, deliveryRole, logGroup, logStream)
        val eventBridgeRole = newEventBridgeRole(deliveryStream)
        newEventRule(deliveryStream, eventBridgeRole)

        newBucketOutputs(bucket)
    }
}
