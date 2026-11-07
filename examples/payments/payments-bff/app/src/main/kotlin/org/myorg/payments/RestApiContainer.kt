package org.myorg.payments

import io.kopipes.aws.AwsGlobalRegistry
import io.kopipes.aws.awsEnvConfig

class RestApiContainer(
    val store: PaymentsStore,
    val clock: () -> Long = System::currentTimeMillis,
) {
    companion object {
        fun build(): RestApiContainer {
            val tableName = awsEnvConfig().entityTableName() ?: error("ENTITY_TABLE_NAME is not configured")
            val client = AwsGlobalRegistry.dynamoDbClientFactory().getClient("restapi")
            return RestApiContainer(store = DynamoDbPaymentsStore(client, tableName))
        }
    }
}
