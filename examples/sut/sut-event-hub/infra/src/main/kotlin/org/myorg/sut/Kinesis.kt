package org.myorg.sut

import software.amazon.awscdk.Duration
import software.amazon.awscdk.services.events.*
import software.amazon.awscdk.services.events.targets.KinesisStream
import software.amazon.awscdk.services.iam.PolicyDocument
import software.amazon.awscdk.services.iam.PolicyStatement
import software.amazon.awscdk.services.iam.Role
import software.amazon.awscdk.services.iam.ServicePrincipal
import software.amazon.awscdk.services.kinesis.Stream
import software.amazon.awscdk.services.kinesis.StreamEncryption

fun EventHubStack.newStream1(): Stream =
    Stream.Builder.create(this, "Stream1")
        .streamName("${service()}-${stage()}-s1")
        .retentionPeriod(Duration.days(1))
        .shardCount(1)
        .encryption(StreamEncryption.MANAGED)
        .build()

fun EventHubStack.newKinesisBusRole(stream: Stream): Role =
    Role.Builder.create(this, "BusRole")
        .roleName("${service()}-${stage()}-bus-s1-role")
        .assumedBy(ServicePrincipal("events.amazonaws.com"))
        .inlinePolicies(
            mapOf(
                "${service()}-${stage()}-internal" to PolicyDocument.Builder.create()
                    .statements(
                        listOf(
                            PolicyStatement.Builder.create()
                                .actions(listOf("kinesis:PutRecord", "kinesis:PutRecords"))
                                .resources(listOf(stream.streamArn))
                                .build()
                        )
                    )
                    .build()
            )
        )
        .build()

fun EventHubStack.publishToKinesis(
    bus: EventBus,
    stream: Stream,
) {
    val busRole = newKinesisBusRole(stream)

    val kinesisTarget = KinesisStream.Builder
        .create(stream)
        .partitionKeyPath("$.detail.partitionKey")
        .message(RuleTargetInput.fromEventPath("$.detail"))
        .build()

    Rule.Builder.create(this, "Stream1EventRule")
        .ruleName("${service()}-${stage()}-bus-s1-rule")
        .eventBus(bus)
        .eventPattern(
            EventPattern.builder()
                .source(Match.anythingBut("external"))
                .detailType(Match.anythingBut("fault"))
                .build()
        )
        .targets(listOf(kinesisTarget))
        .role(busRole)
        .build()
}
