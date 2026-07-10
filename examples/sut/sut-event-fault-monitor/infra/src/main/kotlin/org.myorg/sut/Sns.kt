package org.myorg.sut

import software.amazon.awscdk.Duration
import software.amazon.awscdk.RemovalPolicy
import software.amazon.awscdk.services.iam.AnyPrincipal
import software.amazon.awscdk.services.iam.Effect
import software.amazon.awscdk.services.iam.IGrantable
import software.amazon.awscdk.services.iam.PolicyStatement
import software.amazon.awscdk.services.sns.Topic
import software.amazon.awscdk.services.sns.subscriptions.SqsSubscription
import software.amazon.awscdk.services.sqs.Queue

fun EventFaultMonitorStack.newTopic(): Topic =
    Topic.Builder.create(this, "Topic")
        .topicName("${service()}-${stage()}.fifo")
        .fifo(true)
        .build()

fun EventFaultMonitorStack.newNotificationVerificationQueue(): Queue =
    Queue.Builder.create(this, "NotificationVerificationQueue")
        .queueName("${service()}-${stage()}-notification-verification.fifo")
        .fifo(true)
        .contentBasedDeduplication(true)
        .retentionPeriod(Duration.days(1))
        .removalPolicy(RemovalPolicy.DESTROY)
        .build()

fun EventFaultMonitorStack.subscribeNotificationVerificationQueue(
    topic: Topic,
    queue: Queue,
) {
    queue.addToResourcePolicy(
        PolicyStatement.Builder.create()
            .effect(Effect.ALLOW)
            .principals(listOf(AnyPrincipal()))
            .actions(listOf("sqs:SendMessage"))
            .resources(listOf(queue.queueArn))
            .conditions(
                mapOf(
                    "ArnEquals" to mapOf(
                        "aws:SourceArn" to topic.topicArn,
                    )
                )
            )
            .build()
    )

    topic.addSubscription(
        SqsSubscription.Builder.create(queue)
            .rawMessageDelivery(true)
            .build()
    )
}

fun EventFaultMonitorStack.grantTopicPublish(
    topic: Topic,
    grantable: IGrantable,
) {
    grantable.grantPrincipal.addToPrincipalPolicy(
        PolicyStatement.Builder.create()
            .effect(Effect.ALLOW)
            .actions(
                listOf(
                    "sns:Publish",
                )
            )
            .resources(
                listOf(
                    topic.topicArn,
                )
            )
            .build()
    )
}

