package org.myorg.payments.adapters.inbound

import io.kopipes.aws.AwsKopipesContext
import org.myorg.payments.adapters.outbound.DynamoDbPaymentsStore
import org.myorg.payments.application.Payments
import org.myorg.payments.application.PaymentsService
import org.myorg.payments.application.PaymentsStore

/** Wires the hexagon for the REST function: the use cases on top of a store. */
class RestApiContainer(
    store: PaymentsStore,
    clock: () -> Long = System::currentTimeMillis,
) {
    val payments: Payments = PaymentsService(store, clock)

    companion object {
        fun build(context: AwsKopipesContext = AwsKopipesContext()): RestApiContainer {
            val tableName = context.envConfig.entityTableName() ?: error("ENTITY_TABLE_NAME is not configured")
            val client = context.dynamoDbClientFactory.getClient("restapi")
            return RestApiContainer(store = DynamoDbPaymentsStore(client, tableName))
        }
    }
}
