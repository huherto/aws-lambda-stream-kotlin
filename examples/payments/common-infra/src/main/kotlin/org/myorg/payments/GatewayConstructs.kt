package org.myorg.payments

import software.amazon.awscdk.Duration
import software.amazon.awscdk.RemovalPolicy
import software.amazon.awscdk.services.dynamodb.Attribute
import software.amazon.awscdk.services.dynamodb.AttributeType
import software.amazon.awscdk.services.dynamodb.Billing
import software.amazon.awscdk.services.dynamodb.TableV2
import software.amazon.awscdk.services.lambda.Code
import software.amazon.awscdk.services.lambda.Function
import software.amazon.awscdk.services.lambda.Runtime
import software.amazon.awscdk.services.lambda.eventsources.SqsEventSource
import software.amazon.awscdk.services.sqs.Queue

/**
 * Where each ingress gateway's real input is. In simulation mode the egress gateway delivers the external
 * system's response there, so the two stacks agree on these names; the ingress stack owns the resources.
 */
object Partners {
    /** Stands in for change data capture on core checking's transaction log. */
    fun ledgerTableName(stage: Stage) = "payments-core-checking-ingress-gateway-$stage-ledger"

    /** Where the bank network ingress gateway picks up settlement files. */
    fun bankInboundBucketName(org: String, stage: Stage, region: String) =
        "$org-payments-bank-network-ingress-gateway-$stage-$region"

    /** The card processor ingress gateway's webhook URL. */
    fun cardWebhookUrlParameter(stage: Stage) = "/payments/$stage/card-processor/webhook-url"

    /** The secret the card processor signs its webhooks with. */
    fun cardWebhookSecretParameter(stage: Stage) = "/payments/$stage/card-processor/webhook-secret"
}

/** Simulation mode: on locally, off elsewhere, unless the `simulate` context says otherwise. Never on in production. */
fun BaseStack.simulate(): Boolean {
    val fromContext = node.tryGetContext("simulate")?.toString()?.toBooleanStrictOrNull()
    val simulate = fromContext ?: (stage() == Stage.LOCAL)
    require(!(simulate && stage() == Stage.PROD)) { "Simulation mode must never be on in production" }
    return simulate
}

fun BaseStack.javaFunction(
    name: String,
    handler: String,
    jar: String,
    environment: Map<String, String>,
    timeoutSeconds: Int = 30,
): Function =
    Function.Builder.create(this, name)
        .functionName("${service()}-${stage()}-$name")
        .code(Code.fromAsset("../app/build/libs/$jar"))
        .handler(handler)
        .timeout(Duration.seconds(timeoutSeconds.toLong()))
        .memorySize(1024)
        .runtime(Runtime.JAVA_21)
        .environment(environment)
        .build()

/** The payments an ingress gateway has asked about, joined to the outside system's answers. */
fun BaseStack.requestsTable(): TableV2 =
    TableV2.Builder.create(this, "RequestsTable")
        .tableName("${service()}-${stage()}-requests")
        .partitionKey(Attribute.builder().name("pk").type(AttributeType.STRING).build())
        .sortKey(Attribute.builder().name("sk").type(AttributeType.STRING).build())
        .billing(Billing.onDemand())
        .removalPolicy(RemovalPolicy.DESTROY)
        .build()

/** Holds simulated responses until their delay is over; the simulator function delivers them. */
fun BaseStack.simulationQueue(simulator: Function): Queue {
    val queue = Queue.Builder.create(this, "SimulationQueue")
        .queueName("${service()}-${stage()}-simulation")
        .visibilityTimeout(Duration.seconds(60))
        .build()
    queue.grantConsumeMessages(simulator)
    simulator.addEventSource(SqsEventSource.Builder.create(queue).batchSize(1).build())
    return queue
}
