package org.myorg.sut

import software.amazon.awscdk.services.iam.Role
import software.amazon.awscdk.services.kinesisfirehose.CfnDeliveryStream
import software.amazon.awscdk.services.logs.LogGroup
import software.amazon.awscdk.services.logs.LogStream
import software.amazon.awscdk.services.s3.Bucket
import software.constructs.Construct

class EventLakeS3Stack(scope: Construct, serviceProps: ServiceProps) : BaseStack(scope, serviceProps) {

    val replicateBucket = false

    internal val bucket: Bucket = newBucketWithLifecycle()
    internal val logGroup: LogGroup = newFirehoseLogGroup()
    internal val logStream: LogStream = newFirehoseLogStream(logGroup)
    internal val deliveryRole: Role = newDeliveryRole(bucket, logGroup)
    internal val deliveryStream: CfnDeliveryStream = newDeliveryStream(bucket, deliveryRole, logGroup, logStream)
    internal val eventBridgeRole: Role = newEventBridgeRole(deliveryStream)

    init {
        // Bucket Configuration
        configureBucketLogging(bucket)

        if (replicateBucket) {
            // One of these lines seems to cause some sort of recursivity error.
            val replicationRole = newBucketReplicationRole(bucket)
            grantReplicationAccess(bucket)
            replicateToMirrorRegion(bucket, replicationRole)
        }

        // Event Bridge -> Firehose -> S3
        publishToFirehose(deliveryStream, eventBridgeRole)

        // Outputs
        outputBucketDetails(bucket)
    }
}
