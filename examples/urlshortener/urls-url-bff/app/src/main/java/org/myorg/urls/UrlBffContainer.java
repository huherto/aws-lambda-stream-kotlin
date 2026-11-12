package org.myorg.urls;

import aws.sdk.kotlin.services.dynamodb.DynamoDbClient;
import io.kopipes.aws.AwsKopipesContext;
import io.kopipes.aws.sinks.EventBridgePublisher;
import io.kopipes.core.sinks.EventPublisher;

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
        AwsKopipesContext context = new AwsKopipesContext(UrlsConfig.envConfig());
        DynamoDbClient dynamoDbClient = context.getDynamoDbClientFactory().getClient("urls-url-bff");

        UrlDao urlDao = new UrlDao(dynamoDbClient, tableName);

        EventPublisher eventPublisher = new EventBridgePublisher(
            context.getEnvConfig(), context.getEventBridgeClientFactory(), null, "urls-url-bff");

        return new UrlBffContainer(urlDao, eventPublisher);
    }
}
