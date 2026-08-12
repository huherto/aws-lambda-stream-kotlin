package org.myorg.sut

import software.amazon.awscdk.services.events.CfnRule
import software.amazon.awscdk.services.iam.*
import software.amazon.awscdk.services.kinesisfirehose.CfnDeliveryStream
import software.amazon.awscdk.services.logs.LogGroup
import software.amazon.awscdk.services.logs.LogStream
import software.amazon.awscdk.services.logs.RetentionDays
import software.amazon.awscdk.services.s3.Bucket

fun EventLakeS3Stack.deliveryStreamName(): String =
    "${service()}-${stage()}-DeliveryStream"

fun EventLakeS3Stack.eventBusName(): String =
    "${subsys()}-event-hub-${stage()}-bus"

fun EventLakeS3Stack.logRetentionInDays(): RetentionDays =
    when (stage()) {
        Stage.PROD -> RetentionDays.ONE_MONTH
        else -> RetentionDays.THREE_DAYS
    }

fun EventLakeS3Stack.newFirehoseLogGroup(): LogGroup =
    LogGroup.Builder.create(this, "LogGroup")
        .logGroupName("/aws/kinesisfirehose/${deliveryStreamName()}")
        .retention(logRetentionInDays())
        .build()

fun EventLakeS3Stack.newFirehoseLogStream(logGroup: LogGroup): LogStream =
    LogStream.Builder.create(this, "LogStream")
        .logGroup(logGroup)
        .logStreamName(service())
        .build()

fun EventLakeS3Stack.newDeliveryRole(
    bucket: Bucket,
    logGroup: LogGroup,
): Role =
    Role.Builder.create(this, "DeliveryRole")
        .assumedBy(ServicePrincipal("firehose.amazonaws.com"))
        .inlinePolicies(
            mapOf(
                "delivery" to PolicyDocument.Builder.create()
                    .statements(
                        listOf(
                            PolicyStatement.Builder.create()
                                .effect(Effect.ALLOW)
                                .actions(
                                    listOf(
                                        "s3:AbortMultipartUpload",
                                        "s3:GetBucketLocation",
                                        "s3:GetObject",
                                        "s3:ListBucket",
                                        "s3:ListBucketMultipartUploads",
                                        "s3:PutObject",
                                    )
                                )
                                .resources(
                                    listOf(
                                        bucket.bucketArn,
                                        bucket.arnForObjects("*"),
                                    )
                                )
                                .build(),
                            PolicyStatement.Builder.create()
                                .effect(Effect.ALLOW)
                                .actions(
                                    listOf(
                                        "logs:CreateLogStream",
                                        "logs:CreateLogGroup",
                                        "logs:PutLogEvents",
                                    )
                                )
                                .resources(
                                    listOf(
                                        logGroup.logGroupArn,
                                    )
                                )
                                .build(),
                        )
                    )
                    .build()
            )
        )
        .build()

fun EventLakeS3Stack.newDeliveryStream(
    bucket: Bucket,
    deliveryRole: Role,
    logGroup: LogGroup,
    logStream: LogStream,
): CfnDeliveryStream {
    val deliveryStream = CfnDeliveryStream.Builder.create(this, "DeliveryStream")
        .deliveryStreamName(deliveryStreamName())
        .deliveryStreamType("DirectPut")
        .extendedS3DestinationConfiguration(
            CfnDeliveryStream.ExtendedS3DestinationConfigurationProperty.builder()
                .bucketArn(bucket.bucketArn)
                .prefix("${regionName()}/")
                .bufferingHints(
                    CfnDeliveryStream.BufferingHintsProperty.builder()
                        .intervalInSeconds(60)
                        .sizeInMBs(50)
                        .build()
                )
                .compressionFormat("GZIP")
                .roleArn(deliveryRole.roleArn)
                .cloudWatchLoggingOptions(
                    CfnDeliveryStream.CloudWatchLoggingOptionsProperty.builder()
                        .enabled(true)
                        .logGroupName(logGroup.logGroupName)
                        .logStreamName(logStream.logStreamName)
                        .build()
                )
                .build()
        )
        .build()

    deliveryStream.node.addDependency(deliveryRole)
    deliveryStream.node.addDependency(logStream)

    return deliveryStream
}

fun EventLakeS3Stack.newEventBridgeRole(deliveryStream: CfnDeliveryStream): Role =
    Role.Builder.create(this, "EventBridgeRole")
        .assumedBy(ServicePrincipal("events.amazonaws.com"))
        .inlinePolicies(
            mapOf(
                "put" to PolicyDocument.Builder.create()
                    .statements(
                        listOf(
                            PolicyStatement.Builder.create()
                                .effect(Effect.ALLOW)
                                .actions(
                                    listOf(
                                        "firehose:PutRecord",
                                        "firehose:PutRecordBatch",
                                    )
                                )
                                .resources(
                                    listOf(
                                        deliveryStream.attrArn,
                                    )
                                )
                                .build()
                        )
                    )
                    .build()
            )
        )
        .build()

fun EventLakeS3Stack.publishToFirehose(
    deliveryStream: CfnDeliveryStream,
    eventBridgeRole: Role,
): CfnRule =
    CfnRule.Builder.create(this, "EventRule")
        .eventBusName(eventBusName())
        .eventPattern(
            mapOf(
                "detail" to mapOf(
                    "type" to listOf(
                        mapOf(
                            "anything-but" to "fault"
                        )
                    )
                )
            )
        )
        .state("ENABLED")
        .targets(
            listOf(
                CfnRule.TargetProperty.builder()
                    .id("EventLake")
                    .arn(deliveryStream.attrArn)
                    .roleArn(eventBridgeRole.roleArn)
                    .build()
            )
        )
        .build()