package org.myorg.urls;

import software.amazon.awscdk.Duration;
import software.amazon.awscdk.RemovalPolicy;
import software.amazon.awscdk.services.dynamodb.*;
import software.amazon.awscdk.services.iam.Effect;
import software.amazon.awscdk.services.iam.PolicyStatement;
import software.amazon.awscdk.services.lambda.Code;
import software.amazon.awscdk.services.lambda.Function;
import software.amazon.awscdk.services.lambda.Runtime;
import software.amazon.awscdk.services.lambda.StartingPosition;
import software.amazon.awscdk.services.lambda.eventsources.DynamoEventSource;
import software.constructs.Construct;

import java.util.List;
import java.util.Map;

public class UrlBffStack extends BaseStack {

    private final String tableName;
    private final String busName;
    private final Code jarFile;
    private final Runtime runtime = Runtime.JAVA_21;

    public UrlBffStack(Construct scope, ServiceProps serviceProps) {
        super(scope, serviceProps);

        this.tableName = service() + "-" + stage() + "-table";
        this.busName = subsys() + "-event-hub-" + stage() + "-bus";
        this.jarFile = Code.fromAsset("../app/build/libs/serverless.jar");

        Map<String, String> environment = Map.of(
            "TABLE_NAME", tableName,
            "BUS_NAME", busName,
            "SERIALIZATION_STRATEGY", "JACKSON"
        );

        TableV2 entityTable = TableV2.Builder.create(this, "UrlTable")
                .tableName(tableName)
                .partitionKey(Attribute.builder().name("pk").type(AttributeType.STRING).build())
                .billing(Billing.onDemand())
                .removalPolicy(RemovalPolicy.DESTROY) // For example project
                .dynamoStream(StreamViewType.NEW_AND_OLD_IMAGES)
                .build();

        Function restApi = Function.Builder.create(this, "RestApi")
                .functionName(service() + "-" + stage() + "-restapi")
                .code(jarFile)
                .handler("org.myorg.urls.UrlBffHandler::handleRequest")
                .timeout(Duration.seconds(30))
                .memorySize(1024)
                .runtime(runtime)
                .environment(environment)
                .build();

        Function trigger = Function.Builder.create(this, "Trigger")
                .functionName(service() + "-" + stage() + "-trigger")
                .code(jarFile)
                .handler("org.myorg.urls.UrlTriggerHandler::handleRequest")
                .timeout(Duration.seconds(30))
                .memorySize(1024)
                .runtime(runtime)
                .environment(environment)
                .build();

        entityTable.grantReadWriteData(restApi);
        entityTable.grantStreamRead(trigger);
        
        trigger.addEventSource(DynamoEventSource.Builder.create(entityTable)
                .startingPosition(StartingPosition.TRIM_HORIZON)
                .batchSize(5)
                .bisectBatchOnError(true)
                .build());

        // Grant permission to publish to EventBridge
        restApi.addToRolePolicy(PolicyStatement.Builder.create()
                .effect(Effect.ALLOW)
                .actions(List.of("events:PutEvents"))
                .resources(List.of("arn:aws:events:*:*:event-bus/" + busName))
                .build());

        trigger.addToRolePolicy(PolicyStatement.Builder.create()
                .effect(Effect.ALLOW)
                .actions(List.of("events:PutEvents"))
                .resources(List.of("arn:aws:events:*:*:event-bus/" + busName))
                .build());
    }
}
