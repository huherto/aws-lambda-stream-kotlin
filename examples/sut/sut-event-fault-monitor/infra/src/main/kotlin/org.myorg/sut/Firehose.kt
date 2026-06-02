package org.myorg.sut

import software.amazon.awscdk.Duration
import software.amazon.awscdk.RemovalPolicy
import software.amazon.awscdk.services.events.CfnRule
import software.amazon.awscdk.services.iam.Effect
import software.amazon.awscdk.services.iam.PolicyStatement
import software.amazon.awscdk.services.iam.Role
import software.amazon.awscdk.services.iam.ServicePrincipal
import software.amazon.awscdk.services.kinesisfirehose.CfnDeliveryStream
import software.amazon.awscdk.services.lambda.Function
import software.amazon.awscdk.services.logs.LogGroup
import software.amazon.awscdk.services.logs.LogStream
import software.amazon.awscdk.services.logs.RetentionDays
import software.amazon.awscdk.services.s3.Bucket

fun EventFaultMonitorStack.newLogGroup(): LogGroup =
    LogGroup.Builder.create(this, "LogGroup")
        .logGroupName(logGroupName)
        .retention(RetentionDays.ONE_MONTH)
        .removalPolicy(RemovalPolicy.DESTROY)
        .build()

fun EventFaultMonitorStack.newLogStream(logGroup: LogGroup): LogStream =
    LogStream.Builder.create(this, "LogStream")
        .logGroup(logGroup)
        .logStreamName(service())
        .removalPolicy(RemovalPolicy.DESTROY)
        .build()


fun EventFaultMonitorStack.newTransformLambda(): Function =
    Function.Builder.create(this, "TransformLambdaFunction")
        .functionName("${service()}-${stage()}-transform")
        .code(jarFile)
        .handler("org.myorg.sut.Transform::handleRequest")
        .timeout(Duration.seconds(60))
        .memorySize(1024)
        .runtime(runtime)
        .environment(
            mapOf(
                "JAVA_TOOL_OPTIONS" to "-Dslf4j.provider=io.github.vitalijr2.aws.lambda.slf4j.AWSLambdaServiceProvider",
                "LOG_DEFAULT_LEVEL" to "DEBUG",
            )
        )
        .build()

fun EventFaultMonitorStack.newDeliveryRole(
    bucket: Bucket,
    logGroup: LogGroup,
    transformLambda: Function,
): Role {
    val role = Role.Builder.create(this, "DeliveryRole")
        .assumedBy(ServicePrincipal("firehose.amazonaws.com"))
        .build()

    role.addToPolicy(
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
                    "${bucket.bucketArn}/*",
                )
            )
            .build()
    )

    role.addToPolicy(
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
            .build()
    )

    role.addToPolicy(
        PolicyStatement.Builder.create()
            .effect(Effect.ALLOW)
            .actions(
                listOf(
                    "lambda:InvokeFunction",
                    "lambda:GetFunctionConfiguration",
                )
            )
            .resources(
                listOf(
                    transformLambda.functionArn,
                )
            )
            .build()
    )

    return role
}

fun EventFaultMonitorStack.newDeliveryStream(
    bucket: Bucket,
    deliveryRole: Role,
    logGroup: LogGroup,
    logStream: LogStream,
    transformLambda: Function,
): CfnDeliveryStream =
    CfnDeliveryStream.Builder.create(this, "DeliveryStream")
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
                .processingConfiguration(
                    CfnDeliveryStream.ProcessingConfigurationProperty.builder()
                        .enabled(true)
                        .processors(
                            listOf(
                                CfnDeliveryStream.ProcessorProperty.builder()
                                    .type("Lambda")
                                    .parameters(
                                        listOf(
                                            CfnDeliveryStream.ProcessorParameterProperty.builder()
                                                .parameterName("LambdaArn")
                                                .parameterValue(transformLambda.functionArn)
                                                .build(),
                                            CfnDeliveryStream.ProcessorParameterProperty.builder()
                                                .parameterName("BufferSizeInMBs")
                                                .parameterValue("0.5")
                                                .build(),
                                            CfnDeliveryStream.ProcessorParameterProperty.builder()
                                                .parameterName("BufferIntervalInSeconds")
                                                .parameterValue("60")
                                                .build(),
                                        )
                                    )
                                    .build()
                            )
                        )
                        .build()
                )
                .build()
        )
        .build()

fun EventFaultMonitorStack.newEventBridgeRole(deliveryStream: CfnDeliveryStream): Role {
    val role = Role.Builder.create(this, "EventBridgeRole")
        .assumedBy(ServicePrincipal("events.amazonaws.com"))
        .build()

    role.addToPolicy(
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

    return role
}

fun EventFaultMonitorStack.newEventRule(
    deliveryStream: CfnDeliveryStream,
    eventBridgeRole: Role,
): CfnRule =
    CfnRule.Builder.create(this, "EventRule")
        .eventBusName(busName)
        .eventPattern(
            mapOf(
                "detail" to mapOf(
                    "type" to listOf("fault")
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
                    .inputTransformer(
                        CfnRule.InputTransformerProperty.builder()
                            .inputTemplate("<aws.events.event>\n")
                            .build()
                    )
                    .build()
            )
        )
        .build()