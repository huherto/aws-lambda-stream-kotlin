package org.myorg.payments.adapters.outbound

import aws.sdk.kotlin.services.dynamodb.DynamoDbClient
import aws.sdk.kotlin.services.dynamodb.model.AttributeValue
import aws.sdk.kotlin.services.dynamodb.model.QueryRequest
import kotlinx.serialization.json.Json
import org.myorg.payments.DelayedDelivery
import org.myorg.payments.LedgerEntry
import org.myorg.payments.Payment
import org.myorg.payments.Simulation
import org.myorg.payments.application.CallNotSent
import org.myorg.payments.application.CoreChecking
import org.myorg.payments.simulatedLedgerEntry
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse

/**
 * The real API: `POST /CHK/DEBIT` and `POST /CHK/CREDIT`. Not deployed in this example (core checking lives in
 * another account). A driven adapter for the [CoreChecking] port: the breaker and "called once" wrap it in the
 * application layer, so it only speaks HTTP.
 */
class HttpCoreChecking(private val baseUrl: String, private val http: HttpClient = HttpClient.newHttpClient()) : CoreChecking {
    override suspend fun debit(payment: Payment) = post("/CHK/DEBIT", payment)
    override suspend fun credit(payment: Payment) = post("/CHK/CREDIT", payment)

    /** Assumes an inquiry by reference, `GET /CHK/POSTINGS?REF_NO=…&TXN_TYP=…`, answering 200 or 404. */
    override suspend fun posted(payment: Payment, txnType: String): Boolean {
        val response = http.send(
            HttpRequest.newBuilder(URI.create("$baseUrl/CHK/POSTINGS?REF_NO=${payment.id}&TXN_TYP=$txnType")).GET().build(),
            HttpResponse.BodyHandlers.ofString(),
        )
        return when (response.statusCode()) {
            200 -> true
            404 -> false
            else -> error("core checking inquiry answered ${response.statusCode()}")
        }
    }

    private fun post(path: String, payment: Payment) {
        val body = """{"REF_NO":"${payment.id}","ACCT_NO":"${payment.from.account}","AMT":"${payment.amount}"}"""
        val response = try {
            http.send(
                HttpRequest.newBuilder(URI.create(baseUrl + path))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(body))
                    .build(),
                HttpResponse.BodyHandlers.ofString(),
            )
        } catch (e: java.net.ConnectException) {
            throw CallNotSent("core checking refused the connection", e)
        }
        check(response.statusCode() in 200..299) { "core checking $path answered ${response.statusCode()}" }
    }
}

/**
 * Simulation mode: no call. Schedules the ledger row core checking would have written. In simulation core checking's
 * state *is* the ledger, so the inquiry reads it.
 */
class SimulatedCoreChecking(
    private val delivery: DelayedDelivery,
    private val delaySeconds: Int,
    private val postings: suspend (reference: String, txnType: String) -> Boolean = { _, _ -> false },
    private val outage: SimulatedOutage = SimulatedOutage.none(),
    private val lost: SimulatedOnce = SimulatedOnce.never(),
    private val clock: () -> Long = System::currentTimeMillis,
    /** Core checking numbers every posting itself; a second call is a second posting. */
    private val newEntryId: () -> String = { "E" + java.util.UUID.randomUUID().toString().replace("-", "").take(16).uppercase() },
) : CoreChecking {
    /** C-5 takes core checking down; while it's down, nothing reaches it: the connection is refused. */
    private suspend fun reachable(payment: Payment, starting: Boolean = false) {
        if (starting && payment.from.account == Simulation.OUTAGE_ACCOUNT) outage.startOnce(payment.id, clock())
        if (outage.isDown(clock())) throw CallNotSent("simulated: core checking is down (connection refused)")
    }

    override suspend fun debit(payment: Payment) {
        reachable(payment, starting = true)
        // C-8: the first call is lost on the way; core checking never sees it.
        if (payment.from.account == Simulation.LOST_REQUEST_ACCOUNT && lost.firstTime("lost#${payment.id}")) {
            throw java.net.SocketTimeoutException("simulated: the call timed out and core checking never got it")
        }
        schedule(payment, LedgerEntry.PAYMENT)
        // C-7: the debit is applied, but the answer never comes back.
        if (payment.from.account == Simulation.TIMEOUT_AFTER_DEBIT_ACCOUNT) {
            throw java.net.SocketTimeoutException("simulated: core checking applied the debit but the call timed out")
        }
    }

    override suspend fun credit(payment: Payment) {
        reachable(payment)
        schedule(payment, LedgerEntry.REFUND)
    }

    override suspend fun posted(payment: Payment, txnType: String): Boolean {
        reachable(payment)
        return postings(payment.id, txnType)
    }

    private suspend fun schedule(payment: Payment, txnType: String) {
        val entry = simulatedLedgerEntry(payment, txnType, postedAt = clock() + delaySeconds * 1000L, entryId = newEntryId())
        delivery.deliverLater(Json.encodeToString(LedgerEntry.serializer(), entry), delaySeconds)
    }
}

/** In simulation, core checking's postings are the ledger rows, found by reference through the ledger's index. */
class LedgerPostings(
    private val client: DynamoDbClient,
    private val table: String,
) : suspend (String, String) -> Boolean {
    override suspend fun invoke(reference: String, txnType: String): Boolean =
        client.query(QueryRequest {
            tableName = table
            indexName = "byReference"
            keyConditionExpression = "REF_NO = :r AND TXN_TYP = :t"
            expressionAttributeValues = mapOf(":r" to AttributeValue.S(reference), ":t" to AttributeValue.S(txnType))
            limit = 1
        }).count > 0
}
