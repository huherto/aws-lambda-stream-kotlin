package org.myorg.strack

import software.amazon.awscdk.Duration
import software.amazon.awscdk.RemovalPolicy
import software.amazon.awscdk.services.dynamodb.Attribute
import software.amazon.awscdk.services.dynamodb.AttributeType
import software.amazon.awscdk.services.dynamodb.StreamViewType
import software.amazon.awscdk.services.dynamodb.Table
import software.amazon.awscdk.services.events.EventBus
import software.amazon.awscdk.services.events.EventPattern
import software.amazon.awscdk.services.events.Match
import software.amazon.awscdk.services.events.Rule
import software.amazon.awscdk.services.events.RuleTargetInput
import software.amazon.awscdk.services.events.targets.KinesisStream
import software.amazon.awscdk.services.iam.PolicyDocument
import software.amazon.awscdk.services.iam.PolicyStatement
import software.amazon.awscdk.services.iam.Role
import software.amazon.awscdk.services.iam.ServicePrincipal
import software.amazon.awscdk.services.kinesis.Stream
import software.amazon.awscdk.services.kinesis.StreamEncryption
import software.amazon.awscdk.services.kms.Alias
import software.amazon.awscdk.services.lambda.Code
import software.amazon.awscdk.services.lambda.Function
import software.amazon.awscdk.services.lambda.Runtime
import software.amazon.awscdk.services.lambda.StartingPosition
import software.amazon.awscdk.services.lambda.eventsources.DynamoEventSource
import software.constructs.Construct

class MyStack(scope: Construct, serviceProps: ServiceProps) : BaseStack(scope, serviceProps) {

    // private val myTable: Table = newDynamoDbTable()
    // private  val myLambda: Function = newLambda()
    private val myBus: EventBus = newBus()

    private fun deviceIdKey() =
        Attribute.builder().name("id").type(AttributeType.STRING).build()

    init {
        sendEventsToKinesis()
    }

    private fun newBus() : EventBus = EventBus.Builder
        .create(this, "Bus")
        .eventBusName("${service()}-${stage()}-bus")
        .build()

    private fun sendEventsToKinesis() {

        //val ekey = Alias.fromAliasName(this, "alias/aws/kinesis", "aws/kinesis")

        val stream1 = Stream.Builder.create(this, "Stream1")
            .streamName("${service()}-${stage()}-s1")
            .retentionPeriod(Duration.days(1))
            .shardCount(1)
            .encryption(StreamEncryption.MANAGED)
            //.encryptionKey(ekey)
            .build()

        val appRole: Role = Role.Builder.create(this, "BusRole")
            .assumedBy(ServicePrincipal("events.amazonaws.com"))
//            .managedPolicies(listOf(
//                ManagedPolicy.fromAwsManagedPolicyName("service-role/AWSLambdaBasicExecutionRole")
//            ))
            .inlinePolicies(mapOf(
                "${service()}-${stage()}-internal" to PolicyDocument.Builder.create()
                    .statements(listOf(
                        PolicyStatement.Builder.create()
                            .actions(listOf("kinesis:PutRecord", "kinesis:PutRecords"))
                            .resources(listOf(stream1.streamArn))
                            .build()
                    ))
                    .build()
            ))
            .build()

        val kinesisStream1 = KinesisStream.Builder
            .create(stream1)
            .partitionKeyPath("$.detail.partitionKey")
            .message(RuleTargetInput.fromEventPath("$.detail"))
            .build()

        val kinesisRule =
            Rule.Builder.create(this, "${service()}-${stage()}-rule")
                .eventBus(myBus)
                .eventPattern(
                    EventPattern.builder()
                        .source(Match.anythingBut("external"))
                        .detailType(Match.anythingBut("fault"))
                        .build()
                )
                .targets(listOf(kinesisStream1))
                .role(appRole)
                .build()

    }

    private fun newDynamoDbTable(): Table = Table.Builder
        .create(this, "my-table")
        .tableName("my-table")
        .partitionKey(deviceIdKey())
        .removalPolicy(RemovalPolicy.DESTROY)
        .stream(StreamViewType.NEW_IMAGE)
        .build()

    private fun newLambda(): Function =
        Function.Builder.create(this, "my-lambda")
            .functionName("my-lambda")
            .code(Code.fromAsset("../serverless/build/libs/serverless.jar"))
            .handler("org.myorg.example.MyLambda::handleRequest")
            .timeout(Duration.seconds(50))
            .memorySize(1024)
            .runtime(Runtime.JAVA_21)
            .build()

    private fun addDynamoDBStreamToLambda(function: Function, table: Table) {
        function.addEventSource(
            DynamoEventSource.Builder.create(table)
                .startingPosition(StartingPosition.TRIM_HORIZON)
                .batchSize(5)
                .bisectBatchOnError(true)
                .build()
        )
    }
}
