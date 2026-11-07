package org.myorg.payments

import software.amazon.awscdk.Duration
import software.amazon.awscdk.RemovalPolicy
import software.amazon.awscdk.services.events.EventBus
import software.amazon.awscdk.services.lambda.eventsources.SqsEventSource
import software.amazon.awscdk.services.s3.Bucket
import software.amazon.awscdk.services.s3.EventType
import software.amazon.awscdk.services.s3.NotificationKeyFilter
import software.amazon.awscdk.services.s3.notifications.SqsDestination
import software.amazon.awscdk.services.sqs.Queue
import software.constructs.Construct

/** Hears the bank network: settlement files land in `inbound/`; each line becomes an event. */
class BankNetworkIngressStack(scope: Construct, serviceProps: ServiceProps) : BaseStack(scope, serviceProps) {
    init {
        val jar = "payments-bank-network-ingress-gateway.jar"
        val requests = requestsTable()
        val env = baseLambdaEnvironment() + ("REQUESTS_TABLE_NAME" to requests.tableName)

        val listener = javaFunction("listener", "org.myorg.payments.Listener::handleRequest", jar, env)
        requests.grantReadWriteData(listener)
        consumeFromKinesis(listener, importChannel("DefaultChannel", Channels.DEFAULT))

        val inbound = Bucket.Builder.create(this, "Inbound")
            .bucketName(Partners.bankInboundBucketName(org(), stage(), regionName()))
            .removalPolicy(RemovalPolicy.DESTROY)
            .build()

        val files = Queue.Builder.create(this, "Files")
            .queueName("${service()}-${stage()}-files")
            .visibilityTimeout(Duration.seconds(60))
            .build()
        inbound.addEventNotification(EventType.OBJECT_CREATED, SqsDestination(files), NotificationKeyFilter.builder().prefix("inbound/").build())

        // A file may arrive before its requests were recorded: let the queue redeliver instead of faulting.
        val fileListener = javaFunction(
            "file-listener", "org.myorg.payments.FileListener::handleRequest", jar,
            env + ("STREAM_RETRY_ENABLED" to "true"),
        )
        requests.grantReadData(fileListener)
        inbound.grantRead(fileListener)
        files.grantConsumeMessages(fileListener)
        EventBus.fromEventBusName(this, "Bus", busName()).grantPutEventsTo(fileListener)
        fileListener.addEventSource(SqsEventSource.Builder.create(files).batchSize(1).build())
    }
}
