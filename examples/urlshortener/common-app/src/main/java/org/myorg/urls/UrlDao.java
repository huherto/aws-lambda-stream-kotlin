package org.myorg.urls;

import aws.sdk.kotlin.services.dynamodb.DynamoDbClient;
import aws.sdk.kotlin.services.dynamodb.model.*;
import io.github.huherto.awsLambdaStream.utils.SdkavKt;
import kotlin.coroutines.EmptyCoroutineContext;
import kotlinx.coroutines.BuildersKt;

import java.util.HashMap;
import java.util.Map;

public class UrlDao {
    private final DynamoDbClient dynamoDbClient;
    private final String tableName;

    public UrlDao(DynamoDbClient dynamoDbClient, String tableName) {
        this.dynamoDbClient = dynamoDbClient;
        this.tableName = tableName;
    }

    public String getTableName() {
        return tableName;
    }

    public Url getUrl(String shortUrl) {
        try {
            GetItemResponse response = (GetItemResponse) BuildersKt.runBlocking(EmptyCoroutineContext.INSTANCE, (scope, continuation) ->
                dynamoDbClient.getItem(GetItemRequest.Companion.invoke(b -> {
                    b.setTableName(tableName);
                    b.setKey(Map.of("pk", new AttributeValue.S(shortUrl)));
                    return kotlin.Unit.INSTANCE;
                }), continuation)
            );
            Map<String, AttributeValue> item = response.getItem();
            return item != null ? itemToUrl(item) : null;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    public void saveUrl(Url url) {
        try {
            BuildersKt.runBlocking(EmptyCoroutineContext.INSTANCE, (scope, continuation) ->
                dynamoDbClient.putItem(PutItemRequest.Companion.invoke(b -> {
                    b.setTableName(tableName);
                    b.setItem(urlToItem(url));
                    return kotlin.Unit.INSTANCE;
                }), continuation)
            );
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    public void deleteUrl(String shortUrl) {
        try {
            BuildersKt.runBlocking(EmptyCoroutineContext.INSTANCE, (scope, continuation) ->
                dynamoDbClient.deleteItem(DeleteItemRequest.Companion.invoke(b -> {
                    b.setTableName(tableName);
                    b.setKey(Map.of("pk", new AttributeValue.S(shortUrl)));
                    return kotlin.Unit.INSTANCE;
                }), continuation)
            );
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    public void incrementAccessCount(String shortUrl) {
        try {
            BuildersKt.runBlocking(EmptyCoroutineContext.INSTANCE, (scope, continuation) ->
                dynamoDbClient.updateItem(UpdateItemRequest.Companion.invoke(b -> {
                    b.setTableName(tableName);
                    b.setKey(Map.of("pk", new AttributeValue.S(shortUrl)));
                    b.setUpdateExpression("ADD accessCount :inc");
                    b.setExpressionAttributeValues(Map.of(":inc", new AttributeValue.N("1")));
                    return kotlin.Unit.INSTANCE;
                }), continuation)
            );
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    private Url itemToUrl(Map<String, AttributeValue> item) {
        String shortUrl = item.get("pk").asS();
        String longUrl = item.get("longUrl").asS();
        String accessCountStr = item.get("accessCount").asN();
        Integer accessCount = Integer.parseInt(accessCountStr);
        return new Url(shortUrl, longUrl, accessCount);
    }

    private Map<String, AttributeValue> urlToItem(Url url) {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("pk", new AttributeValue.S(url.shortUrl()));
        item.put("longUrl", new AttributeValue.S(url.longUrl()));
        item.put("accessCount", SdkavKt.nullableN(Double.valueOf(url.accessCount())));
        return item;
    }
}
