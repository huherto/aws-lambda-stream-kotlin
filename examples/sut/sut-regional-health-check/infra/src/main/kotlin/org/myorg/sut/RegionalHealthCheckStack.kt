package org.myorg.sut

import software.amazon.awscdk.services.s3.Bucket
import software.amazon.awscdk.services.sns.Topic
import software.constructs.Construct

class RegionalHealthCheckStack(scope: Construct, serviceProps: ServiceProps) : BaseStack(scope, serviceProps) {

    internal val topic: Topic = newTopic()
    internal val bucket: Bucket = newBucket(topic)

    init {
        newBucketOutputs(bucket)
        createRegionalHealthCheck()
        createSyntheticsCanary(
            bucket = bucket,
            healthCheckEndpoint = node.tryGetContext("healthCheckEndpoint").toString(),
            apiKey = node.tryGetContext("apiKey").toString(),
        )
    }

    fun newTopic(): Topic {
        // This is just a temporal placeholder.
        return Topic(this, "topic")
    }

}