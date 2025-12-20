package org.myorg.sut

import aws.sdk.kotlin.runtime.auth.credentials.EnvironmentCredentialsProvider
import aws.sdk.kotlin.services.dynamodb.DynamoDbClient

fun getDynamoDbClient(): DynamoDbClient? {

    val dynamoDbClient = DynamoDbClient{
        credentialsProvider = EnvironmentCredentialsProvider()
        region = System.getenv("AWS_REGION")
    }
    return dynamoDbClient
}
