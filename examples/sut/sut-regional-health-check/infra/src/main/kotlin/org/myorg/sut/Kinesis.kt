package org.myorg.sut

import software.amazon.awscdk.services.events.CfnRule
import software.amazon.awscdk.services.events.EventBus
import software.amazon.awscdk.services.iam.*
import software.amazon.awscdk.services.kinesis.CfnStream

fun RegionalHealthCheckStack.newStream1(): CfnStream {
    return CfnStream.Builder.create(this, "Stream1")
        .name("${service()}-${stage()}-s1")
        .retentionPeriodHours(24)
        .shardCount(shardCount())
        .streamEncryption(
            CfnStream.StreamEncryptionProperty.builder()
                .encryptionType("KMS")
                .keyId("alias/aws/kinesis")
                .build()
        )
        .build()
}

fun RegionalHealthCheckStack.newKinesisBusRole(stream1: CfnStream): Role =
    Role.Builder.create(this, "BusRole")
        .roleName("${service()}-${stage()}-${regionName()}-kinesis-role")
        .assumedBy(ServicePrincipal("events.amazonaws.com"))
        .inlinePolicies(
            mapOf(
                "${service()}-${stage()}-internal" to PolicyDocument.Builder.create()
                    .statements(
                        listOf(
                            PolicyStatement.Builder.create()
                                .effect(Effect.ALLOW)
                                .actions(
                                    listOf(
                                        "kinesis:PutRecord",
                                        "kinesis:PutRecords",
                                    )
                                )
                                .resources(listOf(stream1.attrArn))
                                .build()
                        )
                    )
                    .build()
            )
        )
        .build()

fun RegionalHealthCheckStack.addStream1EventRule(
    bus: EventBus,
    stream1: CfnStream,
    busRole: Role,
) {
    CfnRule.Builder.create(this, "Stream1EventRule")
        .eventBusName(bus.eventBusName)
        .eventPattern(
            mapOf(
                "detail" to mapOf(
                    "type" to listOf(
                        mapOf("anything-but" to "fault")
                    )
                )
            )
        )
        .state("ENABLED")
        .targets(
            listOf(
                CfnRule.TargetProperty.builder()
                    .id("Stream1")
                    .arn(stream1.attrArn)
                    .roleArn(busRole.roleArn)
                    .kinesisParameters(
                        CfnRule.KinesisParametersProperty.builder()
                            .partitionKeyPath("$.detail.partitionKey")
                            .build()
                    )
                    .inputPath("$.detail")
                    .build()
            )
        )
        .build()
}

fun RegionalHealthCheckStack.kinesisTraceFilterPatterns(): List<Map<String, Any>> =
    listOf(
        mapOf(
            "data" to mapOf(
                "type" to listOf(
                    mapOf("prefix" to "trace-")
                )
            )
        )
    )