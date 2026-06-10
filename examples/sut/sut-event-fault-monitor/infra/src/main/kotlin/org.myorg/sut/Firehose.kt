package org.myorg.sut

import software.amazon.awscdk.Duration
import software.amazon.awscdk.RemovalPolicy
import software.amazon.awscdk.Size
import software.amazon.awscdk.services.events.EventBus
import software.amazon.awscdk.services.events.EventPattern
import software.amazon.awscdk.services.events.Rule
import software.amazon.awscdk.services.events.RuleTargetInput
import software.amazon.awscdk.services.events.targets.FirehoseDeliveryStream
import software.amazon.awscdk.services.iam.Effect
import software.amazon.awscdk.services.iam.PolicyStatement
import software.amazon.awscdk.services.iam.Role
import software.amazon.awscdk.services.iam.ServicePrincipal
import software.amazon.awscdk.services.kinesisfirehose.*
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
        .handler("org.myorg.sut.Transform2::handleRequest")
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
    logGroup: LogGroup,
    logStream: LogStream,
    transformLambda: Function,
): DeliveryStream {
    val processor =
        LambdaFunctionProcessor.Builder
            .create(transformLambda)
            .bufferInterval(Duration.seconds(60))
            .bufferSize(Size.mebibytes(0.5))
            .build()

    val destination = S3Bucket(
        bucket,
        S3BucketProps.builder()
            .dataOutputPrefix("${regionName()}/")
            .bufferingInterval(Duration.seconds(60))
            .bufferingSize(Size.mebibytes(50))
            .compression(Compression.GZIP)
            .loggingConfig(EnableLogging(logGroup))
            .processors(listOf(processor))
            .build()
    )

    return DeliveryStream.Builder
        .create(this, "DeliveryStream")
        .deliveryStreamName(deliveryStreamName)
        .destination(destination)
        .build()
}

fun EventFaultMonitorStack.newEventBridgeRole(deliveryStream: DeliveryStream): Role {
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
                    deliveryStream.deliveryStreamArn,
                )
            )
            .build()
    )

    return role
}

fun EventFaultMonitorStack.newEventRule(
    deliveryStream: DeliveryStream,
    eventBridgeRole: Role,
): Rule {
    val eventBus = EventBus.fromEventBusName(this, "ImportedEventBus", busName)

    val importedDeliveryStream = DeliveryStream.fromDeliveryStreamArn(
        this,
        "ImportedDeliveryStream",
        deliveryStream.deliveryStreamArn,
    )

    val firehoseTarget = FirehoseDeliveryStream.Builder
        .create(importedDeliveryStream)
        .message(RuleTargetInput.fromEventPath("$.detail"))
        .build()

    return Rule.Builder.create(this, "EventRule")
        .eventBus(eventBus)
        .ruleName("${service()}-${stage()}-event-lake-faults-rule")
        .eventPattern(
            EventPattern.builder()
                .detailType(listOf("fault"))
                .build()
        )
        .enabled(true)
        .targets(listOf(firehoseTarget))
        .role(eventBridgeRole)
        .build()
}