package org.myorg.sertrac

import software.amazon.awscdk.Duration
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

fun MyStack.sendEventsToKinesis() {

    val stream1 = Stream.Builder.create(this, "Stream1")
        .streamName("${service()}-${stage()}-s1")
        .retentionPeriod(Duration.days(1))
        .shardCount(1)
        .encryption(StreamEncryption.MANAGED)
        .build()

    val appRole: Role = Role.Builder.create(this, "BusRole")
        .assumedBy(ServicePrincipal("events.amazonaws.com"))
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

    val stream1EventRule =
        Rule.Builder.create(this, "Stream1EventRule")
            .eventBus(this.myBus)
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
