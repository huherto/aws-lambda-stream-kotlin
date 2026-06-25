package org.myorg.sut

import software.amazon.awscdk.services.events.EventBus
import software.amazon.awscdk.services.s3.Bucket
import software.amazon.awscdk.services.sns.Topic
import software.constructs.Construct

class RegionalHealthCheckStack(scope: Construct, serviceProps: ServiceProps) : BaseStack(scope, serviceProps) {

    internal val topic: Topic = newTopic()
    internal val bucket: Bucket = newBucket(topic)
    internal val entitiesTable = newEntitiesTable()
    internal val bus: EventBus = newBus()

    init {
        newBucketOutputs(bucket)
        logBusEventsInCloudWatch(bus)
        createRegionalHealthCheck()
        createSyntheticsCanary(
            bucket = bucket,
            healthCheckEndpoint = node.tryGetContext("healthCheckEndpoint").toString(),
            apiKey = node.tryGetContext("apiKey").toString(),
        )

        // Example once a Lambda consuming/writing the table exists:
        // addEntitiesTablePermissions(myFunction)
        // addEntitiesTableStreamToLambda(myFunction, entitiesTable)
        // addBusPutEventsPermissions(myFunction, bus)
    }

    fun newTopic(): Topic {
        // This is just a temporal placeholder.
        return Topic(this, "topic")
    }

}