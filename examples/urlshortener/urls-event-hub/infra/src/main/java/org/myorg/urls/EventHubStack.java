package org.myorg.urls;

import software.amazon.awscdk.Duration;
import software.amazon.awscdk.services.events.*;
import software.amazon.awscdk.services.events.targets.CloudWatchLogGroup;
import software.amazon.awscdk.services.events.targets.KinesisStream;
import software.amazon.awscdk.services.iam.PolicyDocument;
import software.amazon.awscdk.services.iam.PolicyStatement;
import software.amazon.awscdk.services.iam.Role;
import software.amazon.awscdk.services.iam.ServicePrincipal;
import software.amazon.awscdk.services.kinesis.Stream;
import software.amazon.awscdk.services.kinesis.StreamEncryption;
import software.amazon.awscdk.services.logs.LogGroup;
import software.amazon.awscdk.services.logs.ResourcePolicy;
import software.amazon.awscdk.services.logs.RetentionDays;
import software.constructs.Construct;

import java.util.List;
import java.util.Map;

public class EventHubStack extends BaseStack {

    public final EventBus bus;
    public final Stream stream1;

    public EventHubStack(Construct scope, ServiceProps serviceProps) {
        super(scope, serviceProps);

        this.bus = EventBus.Builder.create(this, "Bus")
                .eventBusName(service() + "-" + stage() + "-bus")
                .build();

        this.stream1 = Stream.Builder.create(this, "Stream1")
                .streamName(service() + "-" + stage() + "-s1")
                .retentionPeriod(Duration.days(1))
                .shardCount(1)
                .encryption(StreamEncryption.MANAGED)
                .build();

        publishToKinesis(bus, stream1);
        logToCloudWatch(bus);
        archiveFromBus(bus);
    }

    private void publishToKinesis(EventBus bus, Stream stream) {
        Role busRole = Role.Builder.create(this, "BusRole")
                .roleName(service() + "-" + stage() + "-bus-s1-role")
                .assumedBy(new ServicePrincipal("events.amazonaws.com"))
                .inlinePolicies(Map.of(
                        service() + "-" + stage() + "-internal",
                        PolicyDocument.Builder.create()
                                .statements(List.of(
                                        PolicyStatement.Builder.create()
                                                .actions(List.of("kinesis:PutRecord", "kinesis:PutRecords"))
                                                .resources(List.of(stream.getStreamArn()))
                                                .build()
                                ))
                                .build()
                ))
                .build();

        KinesisStream kinesisTarget = KinesisStream.Builder.create(stream)
                .partitionKeyPath("$.detail.partitionKey")
                .message(RuleTargetInput.fromEventPath("$.detail"))
                .build();

        Rule.Builder.create(this, "Stream1EventRule")
                .ruleName(service() + "-" + stage() + "-bus-s1-rule")
                .eventBus(bus)
                .eventPattern(EventPattern.builder()
                        .source(Match.anythingBut("external"))
                        .detailType(Match.anythingBut("fault"))
                        .build())
                .targets(List.of(kinesisTarget))
                .role(busRole)
                .build();
    }

    private void logToCloudWatch(EventBus bus) {
        LogGroup logGroupEvents = LogGroup.Builder.create(this, "LogGroupEvents")
                .logGroupName("/aws/events/" + service() + "-" + stage() + "-events")
                .retention(RetentionDays.ONE_MONTH)
                .build();

        LogGroup logGroupFaults = LogGroup.Builder.create(this, "LogGroupFaults")
                .logGroupName("/aws/events/" + service() + "-" + stage() + "-faults")
                .retention(RetentionDays.ONE_MONTH)
                .build();

        ResourcePolicy.Builder.create(this, "LogResourcePolicy")
                .resourcePolicyName(service() + "-" + stage() + "-log")
                .policyStatements(List.of(
                        PolicyStatement.Builder.create()
                                .actions(List.of("logs:CreateLogStream", "logs:PutLogEvents"))
                                .principals(List.of(
                                        new ServicePrincipal("events.amazonaws.com"),
                                        new ServicePrincipal("delivery.logs.amazonaws.com")
                                ))
                                .resources(List.of(logGroupEvents.getLogGroupArn(), logGroupFaults.getLogGroupArn()))
                                .build()
                ))
                .build();

        Rule.Builder.create(this, "LogRuleEvents")
                .eventBus(bus)
                .ruleName(service() + "-" + stage() + "-bus-events-rule")
                .eventPattern(EventPattern.builder()
                        .detailType(Match.anythingBut("fault"))
                        .build())
                .targets(List.of(new CloudWatchLogGroup(logGroupEvents)))
                .build();

        Rule.Builder.create(this, "LogRuleFaults")
                .eventBus(bus)
                .ruleName(service() + "-" + stage() + "-bus-faults-rule")
                .eventPattern(EventPattern.builder()
                        .detailType(List.of("fault"))
                        .build())
                .targets(List.of(new CloudWatchLogGroup(logGroupFaults)))
                .build();
    }

    private void archiveFromBus(EventBus bus) {
        Archive.Builder.create(this, "Archive")
                .archiveName(service() + "-" + stage() + "-archive")
                .sourceEventBus(bus)
                .eventPattern(EventPattern.builder()
                        .detailType(Match.anythingBut("fault"))
                        .build())
                .retention(Duration.days(14))
                .build();
    }
}
