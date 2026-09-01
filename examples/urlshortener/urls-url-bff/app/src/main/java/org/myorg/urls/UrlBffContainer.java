package org.myorg.urls;

import aws.sdk.kotlin.services.dynamodb.DynamoDbClient;
import io.github.huherto.awsLambdaStream.connectors.DefaultDynamoDbClientFactory;
import io.github.huherto.awsLambdaStream.connectors.DefaultEventBridgeClientFactory;
import io.github.huherto.awsLambdaStream.sinks.EventBridgePublisher;
import io.github.huherto.awsLambdaStream.sinks.EventPublisher;

public class UrlBffContainer {
    public final UrlDao urlDao;
    public final EventPublisher eventPublisher;

    public UrlBffContainer(UrlDao urlDao, EventPublisher eventPublisher) {
        this.urlDao = urlDao;
        this.eventPublisher = eventPublisher;
    }

    public static UrlBffContainer build() {
        String tableName = System.getenv("TABLE_NAME");
        if (tableName == null) tableName = "urls-dev-table";
        String busName = System.getenv("BUS_NAME");
        if (busName == null) busName = "urls-dev-bus";

        DynamoDbClient dynamoDbClient = new DefaultDynamoDbClientFactory().getClient("urls-url-bff");
        
        UrlDao urlDao = new UrlDao(dynamoDbClient, tableName);
        
        EventPublisher eventPublisher = new EventBridgePublisher(
            busName,
            "urls-url-bff",
            256 * 1024,
            10,
            8,
            null,
            true,
            new DefaultEventBridgeClientFactory(),
            null
        );

        return new UrlBffContainer(urlDao, eventPublisher);
    }
}
