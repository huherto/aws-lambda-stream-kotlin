package org.myorg.sut

import software.amazon.awscdk.services.events.EventPattern
import software.amazon.awscdk.services.events.Match
import software.amazon.awscdk.services.events.Rule
import software.amazon.awscdk.services.events.targets.CloudWatchLogGroup
import software.amazon.awscdk.services.iam.PolicyStatement
import software.amazon.awscdk.services.iam.ServicePrincipal
import software.amazon.awscdk.services.logs.LogGroup
import software.amazon.awscdk.services.logs.ResourcePolicy
import software.amazon.awscdk.services.logs.RetentionDays

fun MyStack.logEventsInCloudWatch() {

    val logGroupEvents = LogGroup.Builder
        .create(this, "LogGroupEvents")
        .logGroupName("/aws/events/${BaseStack.service()}-${BaseStack.stage()}-events")
        .retention(RetentionDays.ONE_MONTH)
        .build()

    val logGroupFaults = LogGroup.Builder
        .create(this, "LogGroupFaults")
        .logGroupName("/aws/events/${BaseStack.service()}-${BaseStack.stage()}-faults")
        .retention(RetentionDays.ONE_MONTH)
        .build()

    ResourcePolicy.Builder
        .create(this, "LogResourcePolicy")
        .resourcePolicyName("${BaseStack.service()}-${BaseStack.stage()}-log")
        .policyStatements(listOf(
            PolicyStatement.Builder.create()
                .actions(listOf("logs:CreateLogStream", "logs:PutLogEvents"))
                .principals(listOf(
                    ServicePrincipal("events.amazonaws.com"),
                    ServicePrincipal("delivery.logs.amazonaws.com")
                ))
                .resources(listOf(logGroupEvents.logGroupArn, logGroupFaults.logGroupArn))
                .build()
        )).build()

    val logRuleEvents =
        Rule.Builder.create(this, "LogRuleEvents")
            .eventBus(this.myBus)
            .eventPattern(
                EventPattern.builder()
                    .detailType(Match.anythingBut("fault"))
                    .build()
            )
            .targets(listOf(CloudWatchLogGroup(logGroupEvents)))
            .build()

    val logRuleFaults =
        Rule.Builder.create(this, "LogRuleFaults")
            .eventBus(this.myBus)
            .eventPattern(
                EventPattern.builder()
                    .detailType(listOf("fault"))
                    .build()
            )
            .targets(listOf(CloudWatchLogGroup(logGroupFaults)))
            .build()

}