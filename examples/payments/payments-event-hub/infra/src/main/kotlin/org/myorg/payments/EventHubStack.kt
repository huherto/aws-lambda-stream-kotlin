package org.myorg.payments

import software.amazon.awscdk.Duration
import software.amazon.awscdk.services.events.Archive
import software.amazon.awscdk.services.events.EventBus
import software.amazon.awscdk.services.events.EventPattern
import software.amazon.awscdk.services.events.Match
import software.amazon.awscdk.services.events.Rule
import software.amazon.awscdk.services.events.RuleTargetInput
import software.amazon.awscdk.services.events.targets.CloudWatchLogGroup
import software.amazon.awscdk.services.events.targets.KinesisStream
import software.amazon.awscdk.services.iam.PolicyDocument
import software.amazon.awscdk.services.iam.PolicyStatement
import software.amazon.awscdk.services.iam.Role
import software.amazon.awscdk.services.iam.ServicePrincipal
import software.amazon.awscdk.services.kinesis.Stream
import software.amazon.awscdk.services.kinesis.StreamEncryption
import software.amazon.awscdk.services.logs.LogGroup
import software.amazon.awscdk.services.logs.ResourcePolicy
import software.amazon.awscdk.services.logs.RetentionDays
import software.constructs.Construct

/**
 * The subsystem's backbone: producers publish to one bus; routing rules decide where events go.
 *
 * | Rule            | Matches                                         | Goes to                      |
 * |-----------------|-------------------------------------------------|------------------------------|
 * | Default channel | source != external, type != fault               | Kinesis `s1`                 |
 * | Ingress channel | source = external, type starts with `ACCOUNTS.` | Kinesis `ingress`            |
 * | Archive         | type != fault                                   | The bus archive              |
 * | Logs            | everything, faults apart                        | CloudWatch log groups        |
 *
 * The event lake (S3) and the fault monitor add their own rules to this bus.
 * The egress rule (`PAYMENTS.*` to the downstream bus) lives with `payments-egress-gateway`.
 */
class EventHubStack(scope: Construct, serviceProps: ServiceProps) : BaseStack(scope, serviceProps) {

    val bus: EventBus = EventBus.Builder.create(this, "Bus")
        .eventBusName(busName())
        .build()

    val defaultChannel: Stream = newChannel("DefaultChannel", Channels.DEFAULT)
    val ingressChannel: Stream = newChannel("IngressChannel", Channels.INGRESS)

    init {
        val role = newBusRole(listOf(defaultChannel, ingressChannel))

        route(
            id = "DefaultChannelRule",
            ruleName = "${service()}-${stage()}-bus-${Channels.DEFAULT}-rule",
            pattern = EventPattern.builder()
                .source(Match.anythingBut("external"))
                .detailType(Match.anythingBut("fault"))
                .build(),
            stream = defaultChannel,
            role = role,
        )

        // Required here: the default channel excludes external events, so without this rule
        // the ACCOUNTS.* events would reach no stream.
        route(
            id = "IngressChannelRule",
            ruleName = "${service()}-${stage()}-bus-${Channels.INGRESS}-rule",
            pattern = EventPattern.builder()
                .source(listOf("external"))
                .detailType(Match.prefix("ACCOUNTS."))
                .build(),
            stream = ingressChannel,
            role = role,
        )

        logToCloudWatch()
        archive()
    }

    private fun newChannel(id: String, channel: String): Stream =
        Stream.Builder.create(this, id)
            .streamName(channelName(channel))
            // Channels keep events for days: a consumer that is down catches up when it comes back.
            .retentionPeriod(Duration.days(7))
            .shardCount(1)
            .encryption(StreamEncryption.MANAGED)
            .build()

    private fun newBusRole(streams: List<Stream>): Role =
        Role.Builder.create(this, "BusRole")
            .roleName("${service()}-${stage()}-bus-role")
            .assumedBy(ServicePrincipal("events.amazonaws.com"))
            .inlinePolicies(
                mapOf(
                    "${service()}-${stage()}-channels" to PolicyDocument.Builder.create()
                        .statements(
                            listOf(
                                PolicyStatement.Builder.create()
                                    .actions(listOf("kinesis:PutRecord", "kinesis:PutRecords"))
                                    .resources(streams.map { it.streamArn })
                                    .build()
                            )
                        )
                        .build()
                )
            )
            .build()

    private fun route(id: String, ruleName: String, pattern: EventPattern, stream: Stream, role: Role) {
        val target = KinesisStream.Builder.create(stream)
            // A payment's events stay together on one shard.
            .partitionKeyPath("$.detail.partitionKey")
            .message(RuleTargetInput.fromEventPath("$.detail"))
            .build()

        Rule.Builder.create(this, id)
            .ruleName(ruleName)
            .eventBus(bus)
            .eventPattern(pattern)
            .targets(listOf(target))
            .role(role)
            .build()
    }

    private fun logToCloudWatch() {
        val events = LogGroup.Builder.create(this, "LogGroupEvents")
            .logGroupName("/aws/events/${service()}-${stage()}-events")
            .retention(RetentionDays.ONE_MONTH)
            .build()

        val faults = LogGroup.Builder.create(this, "LogGroupFaults")
            .logGroupName("/aws/events/${service()}-${stage()}-faults")
            .retention(RetentionDays.ONE_MONTH)
            .build()

        ResourcePolicy.Builder.create(this, "LogResourcePolicy")
            .resourcePolicyName("${service()}-${stage()}-log")
            .policyStatements(
                listOf(
                    PolicyStatement.Builder.create()
                        .actions(listOf("logs:CreateLogStream", "logs:PutLogEvents"))
                        .principals(
                            listOf(
                                ServicePrincipal("events.amazonaws.com"),
                                ServicePrincipal("delivery.logs.amazonaws.com"),
                            )
                        )
                        .resources(listOf(events.logGroupArn, faults.logGroupArn))
                        .build()
                )
            )
            .build()

        Rule.Builder.create(this, "LogRuleEvents")
            .eventBus(bus)
            .ruleName("${service()}-${stage()}-bus-events-rule")
            .eventPattern(EventPattern.builder().detailType(Match.anythingBut("fault")).build())
            .targets(listOf(CloudWatchLogGroup(events)))
            .build()

        Rule.Builder.create(this, "LogRuleFaults")
            .eventBus(bus)
            .ruleName("${service()}-${stage()}-bus-faults-rule")
            .eventPattern(EventPattern.builder().detailType(listOf("fault")).build())
            .targets(listOf(CloudWatchLogGroup(faults)))
            .build()
    }

    /** The bus archive, the source for replays. Faults are kept by the fault monitor instead. */
    private fun archive() {
        Archive.Builder.create(this, "Archive")
            .archiveName("${service()}-${stage()}-archive")
            .sourceEventBus(bus)
            .eventPattern(EventPattern.builder().detailType(Match.anythingBut("fault")).build())
            .retention(Duration.days(14))
            .build()
    }
}
