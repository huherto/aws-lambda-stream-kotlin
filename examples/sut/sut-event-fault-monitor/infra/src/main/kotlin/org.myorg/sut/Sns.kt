package org.myorg.sut

import software.amazon.awscdk.CfnOutput
import software.amazon.awscdk.services.iam.Effect
import software.amazon.awscdk.services.iam.IGrantable
import software.amazon.awscdk.services.iam.PolicyStatement
import software.amazon.awscdk.services.sns.Topic

fun EventFaultMonitorStack.newTopic(): Topic =
    Topic.Builder.create(this, "Topic")
        .topicName("${service()}-${stage()}.fifo")
        .fifo(true)
        .build()

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

fun EventFaultMonitorStack.newTopicOutputs(topic: Topic) {
    CfnOutput.Builder.create(this, "TopicArn")
        .value(topic.topicArn)
        .build()
}
