package org.myorg.payments

import software.amazon.awscdk.CfnOutput
import software.amazon.awscdk.Duration
import software.amazon.awscdk.RemovalPolicy
import software.amazon.awscdk.services.apigateway.LambdaIntegration
import software.amazon.awscdk.services.apigateway.RestApi
import software.amazon.awscdk.services.apigateway.StageOptions
import software.amazon.awscdk.services.dynamodb.Attribute
import software.amazon.awscdk.services.dynamodb.AttributeType
import software.amazon.awscdk.services.dynamodb.Billing
import software.amazon.awscdk.services.dynamodb.GlobalSecondaryIndexPropsV2
import software.amazon.awscdk.services.dynamodb.ProjectionType
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
 * The payment BFF: REST API → table → (stream) → trigger → bus, and default channel → listener → table.
 */
class PaymentBffStack(scope: Construct, serviceProps: ServiceProps) : BaseStack(scope, serviceProps) {

    private val jarFile: Code = Code.fromAsset("../app/build/libs/payments-bff.jar")
    private val runtime: Runtime = Runtime.JAVA_21

    val table: TableV2 = TableV2.Builder.create(this, "PaymentsTable")
        .tableName("${service()}-${stage()}-payments")
        .partitionKey(Attribute.builder().name("pk").type(AttributeType.STRING).build())
        .sortKey(Attribute.builder().name("sk").type(AttributeType.STRING).build())
        .billing(Billing.onDemand())
        .removalPolicy(RemovalPolicy.RETAIN)
        .dynamoStream(StreamViewType.NEW_AND_OLD_IMAGES)
        // Sparse: only accepted payments have both customerId and submittedAt.
        .globalSecondaryIndexes(
            listOf(
                GlobalSecondaryIndexPropsV2.builder()
                    .indexName("byCustomer")
                    .partitionKey(Attribute.builder().name("customerId").type(AttributeType.STRING).build())
                    .sortKey(Attribute.builder().name("submittedAt").type(AttributeType.NUMBER).build())
                    .projectionType(ProjectionType.ALL)
                    .build()
            )
        )
        .build()

    private val environment = baseLambdaEnvironment() + mapOf("ENTITY_TABLE_NAME" to table.tableName)

    init {
        val bus = EventBus.fromEventBusName(this, "Bus", busName())

        val restApi = newFunction("restapi", "org.myorg.payments.RestApi::handleRequest")
        table.grantReadWriteData(restApi)

        val trigger = newFunction("trigger", "org.myorg.payments.Trigger::handleRequest")
        table.grantStreamRead(trigger)
        bus.grantPutEventsTo(trigger)
        trigger.addEventSource(
            DynamoEventSource.Builder.create(table)
                .startingPosition(StartingPosition.TRIM_HORIZON)
                .batchSize(25)
                .bisectBatchOnError(true)
                .retryAttempts(3)
                .build()
        )

        val listener = newFunction("listener", "org.myorg.payments.Listener::handleRequest")
        table.grantReadWriteData(listener)
        consumeFromKinesis(listener, importChannel("DefaultChannel", Channels.DEFAULT))

        val api = RestApi.Builder.create(this, "Api")
            .restApiName("${service()}-${stage()}-api")
            .deployOptions(StageOptions.builder().stageName(stage().id).build())
            .build()
        val integration = LambdaIntegration(restApi)
        val payments = api.root.addResource("payments")
        payments.addMethod("GET", integration)
        val payment = payments.addResource("{id}")
        payment.addMethod("GET", integration)
        payment.addMethod("PUT", integration)

        CfnOutput.Builder.create(this, "ApiId").value(api.restApiId).build()
        CfnOutput.Builder.create(this, "ApiUrl").value(api.url).build()
    }

    private fun newFunction(name: String, handler: String): Function =
        Function.Builder.create(this, name)
            .functionName("${service()}-${stage()}-$name")
            .code(jarFile)
            .handler(handler)
            .timeout(Duration.seconds(30))
            .memorySize(1024)
            .runtime(runtime)
            .environment(environment)
            .build()
}
