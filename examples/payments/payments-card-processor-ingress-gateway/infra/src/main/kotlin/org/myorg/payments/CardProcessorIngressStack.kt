package org.myorg.payments

import software.amazon.awscdk.CfnOutput
import software.amazon.awscdk.services.apigateway.LambdaIntegration
import software.amazon.awscdk.services.apigateway.RestApi
import software.amazon.awscdk.services.apigateway.StageOptions
import software.amazon.awscdk.services.events.EventBus
import software.amazon.awscdk.services.ssm.StringParameter
import software.constructs.Construct

/**
 * Hears the card processor through its webhook, `POST /webhooks/card-processor`. Publishes the webhook URL and
 * signing secret as parameters: the processor is configured with them, and so is the egress gateway in simulation.
 *
 * The secret comes from the `cardWebhookSecret` context. A real deployment keeps it in a secrets store and reads it
 * at runtime; an environment variable keeps the example small.
 */
class CardProcessorIngressStack(scope: Construct, serviceProps: ServiceProps) : BaseStack(scope, serviceProps) {
    init {
        val jar = "payments-card-processor-ingress-gateway.jar"
        val secret = node.tryGetContext("cardWebhookSecret")?.toString() ?: "demo-signing-secret-${stage()}"
        val requests = requestsTable()
        val env = baseLambdaEnvironment() + ("REQUESTS_TABLE_NAME" to requests.tableName)

        val listener = javaFunction("listener", "org.myorg.payments.Listener::handleRequest", jar, env)
        requests.grantReadWriteData(listener)
        consumeFromKinesis(listener, importChannel("DefaultChannel", Channels.DEFAULT))

        val webhook = javaFunction("webhook", "org.myorg.payments.Webhook::handleRequest", jar, env + ("WEBHOOK_SECRET" to secret))
        requests.grantReadData(webhook)
        EventBus.fromEventBusName(this, "Bus", busName()).grantPutEventsTo(webhook)

        val api = RestApi.Builder.create(this, "Api")
            .restApiName("${service()}-${stage()}-api")
            .deployOptions(StageOptions.builder().stageName(stage().id).build())
            .build()
        api.root.addResource("webhooks").addResource("card-processor").addMethod("POST", LambdaIntegration(webhook))

        val url = api.urlForPath("/webhooks/card-processor")
        StringParameter.Builder.create(this, "WebhookUrl")
            .parameterName(Partners.cardWebhookUrlParameter(stage()))
            .stringValue(url)
            .build()
        StringParameter.Builder.create(this, "WebhookSecret")
            .parameterName(Partners.cardWebhookSecretParameter(stage()))
            .stringValue(secret)
            .build()
        CfnOutput.Builder.create(this, "WebhookUrlOutput").value(url).build()
    }
}
