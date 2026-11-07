package org.myorg.payments

import software.amazon.awscdk.Duration
import software.amazon.awscdk.RemovalPolicy
import software.amazon.awscdk.services.dynamodb.Attribute
import software.amazon.awscdk.services.dynamodb.AttributeType
import software.amazon.awscdk.services.dynamodb.Billing
import software.amazon.awscdk.services.dynamodb.StreamViewType
import software.amazon.awscdk.services.dynamodb.TableV2
import software.amazon.awscdk.services.events.EventBus
import software.amazon.awscdk.services.lambda.Code
import software.amazon.awscdk.services.lambda.Function
import software.amazon.awscdk.services.lambda.Runtime
import software.amazon.awscdk.services.lambda.StartingPosition
import software.amazon.awscdk.services.lambda.eventsources.DynamoEventSource
import software.constructs.Construct

/**
 * The payment control service: no REST API, it only consumes and produces events.
 *
 * Default channel → listener → events table → (stream) → trigger → bus.
 * The table's TTL deletes each payment's deadline item at the deadline; the trigger turns that into PAYMENT_EXPIRED.
 */
class ControlServiceStack(scope: Construct, serviceProps: ServiceProps) : BaseStack(scope, serviceProps) {

    private val jarFile: Code = Code.fromAsset("../app/build/libs/payments-control-service.jar")
    private val runtime: Runtime = Runtime.JAVA_21

    /** 2 days in production; short locally, so PAYMENT_EXPIRED shows up in a demo. */
    private val deadlineSeconds: Long = if (stage() == Stage.LOCAL) 120 else 2 * 24 * 60 * 60

    val eventsTable: TableV2 = TableV2.Builder.create(this, "EventsTable")
        .tableName("${service()}-${stage()}-events")
        .partitionKey(Attribute.builder().name("pk").type(AttributeType.STRING).build())
        .sortKey(Attribute.builder().name("sk").type(AttributeType.STRING).build())
        .billing(Billing.onDemand())
        .removalPolicy(RemovalPolicy.RETAIN)
        // Deletes old events and, at each payment's deadline, its deadline item.
        .timeToLiveAttribute("ttl")
        // REMOVE records must carry the old image: the deadline item's payment is in it.
        .dynamoStream(StreamViewType.NEW_AND_OLD_IMAGES)
        .build()

    private val environment = baseLambdaEnvironment() + mapOf(
        "EVENT_TABLE_NAME" to eventsTable.tableName,
        "DEADLINE_SECONDS" to deadlineSeconds.toString(),
    )

    init {
        val bus = EventBus.fromEventBusName(this, "Bus", busName())

        val listener = newFunction("listener", "org.myorg.payments.Listener::handleRequest")
        eventsTable.grantReadWriteData(listener)
        consumeFromKinesis(listener, importChannel("DefaultChannel", Channels.DEFAULT))

        val trigger = newFunction("trigger", "org.myorg.payments.Trigger::handleRequest")
        eventsTable.grantReadWriteData(trigger)
        eventsTable.grantStreamRead(trigger)
        bus.grantPutEventsTo(trigger)
        trigger.addEventSource(
            DynamoEventSource.Builder.create(eventsTable)
                .startingPosition(StartingPosition.TRIM_HORIZON)
                .batchSize(25)
                .bisectBatchOnError(true)
                .retryAttempts(3)
                .build()
        )
    }

    private fun newFunction(name: String, handler: String): Function =
        Function.Builder.create(this, name)
            .functionName("${service()}-${stage()}-$name")
            .code(jarFile)
            .handler(handler)
            .timeout(Duration.seconds(50))
            .memorySize(1024)
            .runtime(runtime)
            .environment(environment)
            .build()
}
