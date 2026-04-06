package io.github.huherto.awsLambdaStream

import aws.sdk.kotlin.runtime.auth.credentials.EnvironmentCredentialsProvider
import aws.sdk.kotlin.services.dynamodb.DynamoDbClient
import aws.smithy.kotlin.runtime.net.url.Url

fun getDynamoDbClient(envConfig : EnvironmentConfig): DynamoDbClient {
    val endpointUrl = envConfig.endPointUrl()
    val region = envConfig.awsRegion()

    return DynamoDbClient {
        // Explicitly set the region
        this.region = region
        this.credentialsProvider = EnvironmentCredentialsProvider()
        
        // If an endpoint URL is provided (like http://localhost:4566), use it
        endpointUrl?.let { this.endpointUrl = Url.parse(it) }
    }
}