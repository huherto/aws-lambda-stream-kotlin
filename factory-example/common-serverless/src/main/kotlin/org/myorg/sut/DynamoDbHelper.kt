package org.myorg.sut

import software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider
import software.amazon.awssdk.core.SdkSystemSetting
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient
import software.amazon.awssdk.services.dynamodb.DynamoDbClient

fun getEnhancedClient(): DynamoDbEnhancedClient? {
    val ddb: DynamoDbClient? = getDynamoDbClient()
    val enhancedClient = DynamoDbEnhancedClient.builder()
        .dynamoDbClient(ddb)
        .build()
    return enhancedClient;
}

fun getDynamoDbClient(): DynamoDbClient? {
    val dynamoDbClient = DynamoDbClient.builder()
        .credentialsProvider(EnvironmentVariableCredentialsProvider.create())
        .region(Region.of(System.getenv(SdkSystemSetting.AWS_REGION.environmentVariable())))
        .overrideConfiguration(
            ClientOverrideConfiguration.builder()
                .build()
        )
        .build()
    return dynamoDbClient
}
