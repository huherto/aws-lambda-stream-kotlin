package org.myorg.urls;

import software.amazon.awscdk.Aws;
import software.amazon.awscdk.Duration;
import software.amazon.awscdk.RemovalPolicy;
import software.amazon.awscdk.services.dynamodb.*;
import software.amazon.awscdk.services.kinesis.Stream;
import software.amazon.awscdk.services.lambda.Code;
import software.amazon.awscdk.services.lambda.Function;
import software.amazon.awscdk.services.lambda.Runtime;
import software.amazon.awscdk.services.lambda.StartingPosition;
import software.amazon.awscdk.services.lambda.eventsources.DynamoEventSource;
import software.amazon.awscdk.services.lambda.eventsources.KinesisEventSource;
import software.constructs.Construct;

import java.util.Map;

public class ControlServiceStack extends BaseStack {

    public ControlServiceStack(Construct scope, ServiceProps serviceProps) {
        super(scope, serviceProps);

        String eventsTableName = service() + "-" + stage() + "-events";
        String urlsTableName = subsys() + "-url-bff-" + stage() + "-table";
        String streamName = subsys() + "-event-hub-" + stage() + "-s1";

        Code jarFile = Code.fromAsset("../app/build/libs/serverless.jar");
        Runtime runtime = Runtime.JAVA_21;

        Map<String, String> environment = Map.of(
            "EVENTS_TABLE_NAME", eventsTableName,
            "URLS_TABLE_NAME", urlsTableName,
            "SERIALIZATION_STRATEGY", "JACKSON"
        );

        TableV2 eventsTable = TableV2.Builder.create(this, "EventsTable")
                .tableName(eventsTableName)
                .partitionKey(Attribute.builder().name("pk").type(AttributeType.STRING).build())
                .sortKey(Attribute.builder().name("sk").type(AttributeType.STRING).build())
                .billing(Billing.onDemand())
                .removalPolicy(RemovalPolicy.DESTROY)
                .dynamoStream(StreamViewType.NEW_AND_OLD_IMAGES)
                .build();

        Function listener = Function.Builder.create(this, "Listener")
                .functionName(service() + "-" + stage() + "-listener")
                .code(jarFile)
                .handler("org.myorg.urls.ControlListenerHandler::handleRequest")
                .timeout(Duration.seconds(30))
                .memorySize(1024)
                .runtime(runtime)
                .environment(environment)
                .build();

        Function trigger = Function.Builder.create(this, "Trigger")
                .functionName(service() + "-" + stage() + "-trigger")
                .code(jarFile)
                .handler("org.myorg.urls.ControlTriggerHandler::handleRequest")
                .timeout(Duration.seconds(30))
                .memorySize(1024)
                .runtime(runtime)
                .environment(environment)
                .build();

        eventsTable.grantReadWriteData(listener);
        eventsTable.grantStreamRead(trigger);

        // Grant permission to update stats in URL table
        ITableV2 urlsTable = TableV2.fromTableName(this, "UrlsTable", urlsTableName);
        urlsTable.grantReadWriteData(trigger);

        // Consume from Kinesis
        String streamArn = "arn:aws:kinesis:" + Aws.REGION + ":" + Aws.ACCOUNT_ID + ":stream/" + streamName;
        Stream stream1 = (Stream) Stream.fromStreamArn(this, "Stream1", streamArn);

        listener.addEventSource(KinesisEventSource.Builder.create(stream1)
                .startingPosition(StartingPosition.LATEST)
                .batchSize(100)
                .build());

        // Consume from Events Microstore stream
        trigger.addEventSource(DynamoEventSource.Builder.create(eventsTable)
                .startingPosition(StartingPosition.TRIM_HORIZON)
                .batchSize(5)
                .build());
    }
}
