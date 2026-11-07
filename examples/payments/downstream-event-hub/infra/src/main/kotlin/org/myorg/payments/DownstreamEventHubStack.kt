package org.myorg.payments

import software.amazon.awscdk.Duration
import software.amazon.awscdk.services.events.EventBus
import software.amazon.awscdk.services.events.EventPattern
import software.amazon.awscdk.services.events.Match
import software.amazon.awscdk.services.events.Rule
import software.amazon.awscdk.services.events.targets.CloudWatchLogGroup
import software.amazon.awscdk.services.events.targets.SqsQueue
import software.amazon.awscdk.services.sqs.Queue
import software.amazon.awscdk.services.logs.LogGroup
import software.amazon.awscdk.services.logs.RetentionDays
import software.constructs.Construct

/**
 * Stands in for any downstream subsystem's event hub: a second bus and a catch-all rule to a log, so learners
 * read exactly what crossed the boundary. Same account as ours; the payments egress gateway's rule targets it.
 */
class DownstreamEventHubStack(scope: Construct, serviceProps: ServiceProps) : BaseStack(scope, serviceProps) {
    init {
        // subsys is "downstream", so this is downstream-event-hub-<stage>-bus.
        val bus = EventBus.Builder.create(this, "Bus").eventBusName(busName()).build()

        val log = LogGroup.Builder.create(this, "Log")
            .logGroupName("/aws/events/${service()}-${stage()}-events")
            .retention(RetentionDays.ONE_MONTH)
            .build()

        Rule.Builder.create(this, "CatchAll")
            .ruleName("${service()}-${stage()}-catch-all")
            .eventBus(bus)
            .eventPattern(EventPattern.builder().source(Match.prefix("")).build())
            .targets(listOf(CloudWatchLogGroup(log)))
            .build()

        // Lets the integration tests read what crossed the boundary, as the sut example's fault monitor does.
        val verification = Queue.Builder.create(this, "VerificationQueue")
            .queueName("${service()}-${stage()}-verification")
            .retentionPeriod(Duration.hours(1))
            .build()
        Rule.Builder.create(this, "CatchAllToVerification")
            .ruleName("${service()}-${stage()}-catch-all-verification")
            .eventBus(bus)
            .eventPattern(EventPattern.builder().source(Match.prefix("")).build())
            .targets(listOf(SqsQueue(verification)))
            .build()
    }
}
