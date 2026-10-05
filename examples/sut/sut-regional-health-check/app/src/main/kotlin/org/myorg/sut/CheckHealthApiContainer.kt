package org.myorg.sut

import aws.sdk.kotlin.services.dynamodb.DynamoDbClient
import io.kopipes.aws.awsEnvConfig
import kotlinx.coroutines.runBlocking

class CheckHealthApiContainer(
    val dynamoDBClient: DynamoDbClient
) {

    companion object {

        fun build(): CheckHealthApiContainer {
            val dynamoDbClient = runBlocking {
                DynamoDbClient.fromEnvironment {}
            }

            return CheckHealthApiContainer(dynamoDBClient = dynamoDbClient)
        }
    }

    val tableName = awsEnvConfig().entityTableName()
        ?: error("ENTITY_TABLE_NAME is not configured")

    val unhealthyFlag : Boolean = awsEnvConfig().unhealthy()

    val awsRegion : String = awsEnvConfig().awsRegion()

    private fun debug(namespace: String): (String) -> Unit =
        { message ->
            println("[$namespace] $message")
        }

    val connector = Connector(
        debug = debug("connector"),
        tableName = tableName,
        db = dynamoDBClient,
    )

    val tracerDao = TracerDao(
        connector = connector,
        awsRegion = awsRegion
    )

}
