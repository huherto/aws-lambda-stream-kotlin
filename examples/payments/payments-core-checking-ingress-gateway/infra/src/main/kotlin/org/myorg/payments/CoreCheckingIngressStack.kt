package org.myorg.payments

import software.amazon.awscdk.RemovalPolicy
import software.amazon.awscdk.services.dynamodb.Attribute
import software.amazon.awscdk.services.dynamodb.AttributeType
import software.amazon.awscdk.services.dynamodb.Billing
import software.amazon.awscdk.services.dynamodb.StreamViewType
import software.amazon.awscdk.services.dynamodb.TableV2
import software.amazon.awscdk.services.events.EventBus
import software.amazon.awscdk.services.lambda.StartingPosition
import software.amazon.awscdk.services.lambda.eventsources.DynamoEventSource
import software.constructs.Construct

/**
 * Hears core checking. The `ledger` table stands in for change data capture on core checking's transaction log
 * (the book uses a replication tool on the real log; heavy for a demo). In simulation mode the egress gateway
 * writes the rows core checking would have written.
 */
class CoreCheckingIngressStack(scope: Construct, serviceProps: ServiceProps) : BaseStack(scope, serviceProps) {
    init {
        val jar = "payments-core-checking-ingress-gateway.jar"
        val requests = requestsTable()
        val env = baseLambdaEnvironment() + ("REQUESTS_TABLE_NAME" to requests.tableName)

        val listener = javaFunction("listener", "org.myorg.payments.Listener::handleRequest", jar, env)
        requests.grantReadWriteData(listener)
        consumeFromKinesis(listener, importChannel("DefaultChannel", Channels.DEFAULT))

        val ledger = TableV2.Builder.create(this, "Ledger")
            .tableName(Partners.ledgerTableName(stage()))
            .partitionKey(Attribute.builder().name("ENTRY_ID").type(AttributeType.STRING).build())
            .billing(Billing.onDemand())
            .removalPolicy(RemovalPolicy.DESTROY)
            .dynamoStream(StreamViewType.NEW_IMAGE)
            .build()

        // A row may arrive before its request was recorded: retry the batch instead of faulting.
        val trigger = javaFunction(
            "ledger-trigger", "org.myorg.payments.LedgerTrigger::handleRequest", jar,
            env + ("STREAM_RETRY_ENABLED" to "true"),
        )
        requests.grantReadData(trigger)
        ledger.grantStreamRead(trigger)
        EventBus.fromEventBusName(this, "Bus", busName()).grantPutEventsTo(trigger)
        trigger.addEventSource(
            DynamoEventSource.Builder.create(ledger)
                .startingPosition(StartingPosition.TRIM_HORIZON)
                .batchSize(10)
                .bisectBatchOnError(true)
                .retryAttempts(10)
                .build()
        )
    }
}
