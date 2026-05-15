package org.myorg.sut

import aws.sdk.kotlin.services.dynamodb.DynamoDbClient
import io.github.huherto.awsLambdaStream.EnvironmentConfig
import kotlinx.coroutines.runBlocking

class RestApiContainer(
    val envConfig: EnvironmentConfig,
    val dynamoDBClient: DynamoDbClient) {

    companion object {

        fun build(): RestApiContainer {
            val dynamoDbClient = runBlocking {
                DynamoDbClient.fromEnvironment {}
            }
            val envConfig = EnvironmentConfig()

            return RestApiContainer(envConfig, dynamoDBClient = dynamoDbClient)
        }
    }

    val tableName = envConfig.entityTableName()
        ?: error("ENTITY_TABLE_NAME is not configured")

}