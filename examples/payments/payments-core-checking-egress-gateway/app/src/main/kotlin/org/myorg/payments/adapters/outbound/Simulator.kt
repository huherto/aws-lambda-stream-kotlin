package org.myorg.payments.adapters.outbound

import aws.sdk.kotlin.services.dynamodb.model.AttributeValue
import aws.sdk.kotlin.services.dynamodb.model.PutItemRequest
import com.amazonaws.services.lambda.runtime.Context
import com.amazonaws.services.lambda.runtime.RequestHandler
import com.amazonaws.services.lambda.runtime.events.SQSEvent
import io.kopipes.aws.AwsKopipesContext
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.Json
import mu.KotlinLogging
import org.myorg.payments.LedgerEntry

/**
 * Simulation only: plays core checking's part. Writes the delayed ledger row to the change feed the ingress gateway
 * reads. It is the far side of [SimulatedCoreChecking], not part of the gateway's hexagon.
 */
class Simulator : RequestHandler<SQSEvent, Void?> {
    private val logger = KotlinLogging.logger { }
    private val ledgerTable = System.getenv("LEDGER_TABLE_NAME") ?: "undefined"
    private val kopipes = AwsKopipesContext()

    override fun handleRequest(event: SQSEvent, context: Context): Void? = runBlocking {
        val client = kopipes.dynamoDbClientFactory.getClient("simulator")
        for (message in event.records) {
            val entry = Json.decodeFromString(LedgerEntry.serializer(), message.body)
            client.putItem(PutItemRequest {
                tableName = ledgerTable
                item = entry.toAttributes().mapValues { AttributeValue.S(it.value) }
            })
            logger.info { "simulated ledger row ${entry.ENTRY_ID} ${entry.STAT}" }
        }
        null
    }
}
