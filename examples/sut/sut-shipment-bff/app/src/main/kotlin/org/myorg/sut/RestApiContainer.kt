package org.myorg.sut

import aws.sdk.kotlin.services.dynamodb.DynamoDbClient
import io.kopipes.aws.AwsEnvironmentConfig
import kotlinx.coroutines.runBlocking

class RestApiContainer(
    envConfig: AwsEnvironmentConfig,
    val dynamoDBClient: DynamoDbClient) {

    companion object {

        fun build(): RestApiContainer {
            val dynamoDbClient = runBlocking {
                DynamoDbClient.fromEnvironment {}
            }
            val envConfig = AwsEnvironmentConfig()

            return RestApiContainer(envConfig, dynamoDBClient = dynamoDbClient)
        }
    }

    val tableName = envConfig.entityTableName()
        ?: error("ENTITY_TABLE_NAME is not configured")

    val shipmentDao: ShipmentDao = ShipmentDao(dynamoDBClient, tableName)

}
