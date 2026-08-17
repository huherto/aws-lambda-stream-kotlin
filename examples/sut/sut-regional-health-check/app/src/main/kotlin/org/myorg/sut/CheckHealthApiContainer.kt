package org.myorg.sut

import aws.sdk.kotlin.services.dynamodb.DynamoDbClient
import io.github.huherto.awsLambdaStream.GlobalRegistry.envConfig
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

    val tableName = envConfig().entityTableName()
        ?: error("ENTITY_TABLE_NAME is not configured")

    val unhealthyFlag : Boolean = envConfig().unhealthy()

    val awsRegion : String = envConfig().awsRegion()

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