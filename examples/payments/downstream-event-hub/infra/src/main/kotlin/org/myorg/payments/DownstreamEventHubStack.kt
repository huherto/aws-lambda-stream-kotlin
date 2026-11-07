package org.myorg.payments

import software.amazon.awscdk.services.events.EventBus
import software.amazon.awscdk.services.events.EventPattern
import software.amazon.awscdk.services.events.Match
import software.amazon.awscdk.services.events.Rule
import software.amazon.awscdk.services.events.targets.CloudWatchLogGroup
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
    }
}
