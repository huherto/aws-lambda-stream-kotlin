package org.myorg.payments

import aws.sdk.kotlin.runtime.auth.credentials.StaticCredentialsProvider
import aws.sdk.kotlin.services.dynamodb.DynamoDbClient
import aws.sdk.kotlin.services.dynamodb.model.AttributeValue
import aws.sdk.kotlin.services.dynamodb.model.DeleteItemRequest
import aws.sdk.kotlin.services.dynamodb.model.GetItemRequest
import aws.sdk.kotlin.services.dynamodb.model.QueryRequest
import aws.sdk.kotlin.services.dynamodb.model.UpdateItemRequest
import aws.sdk.kotlin.services.lambda.LambdaClient
import aws.sdk.kotlin.services.s3.S3Client
import aws.sdk.kotlin.services.s3.model.GetObjectRequest
import aws.sdk.kotlin.services.s3.model.ListObjectsV2Request
import aws.smithy.kotlin.runtime.content.decodeToString
import aws.smithy.kotlin.runtime.net.url.Url
import io.kopipes.aws.tools.ResubmitFaults
import kotlinx.coroutines.runBlocking
import kotlinx.datetime.Clock
import kotlinx.datetime.TimeZone
import kotlinx.datetime.toLocalDateTime
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlin.system.exitProcess

/** A fault as the fault monitor stored it: where it happened and why. */
data class StoredFault(
    val time: String,
    val functionName: String,
    val pipeline: String,
    val error: String,
    val objectKey: String,
)

/**
 * What an operator does with faults in the payments subsystem: see them, fix their cause, resubmit them.
 * Works against LocalStack (or AWS, with the endpoint unset and real credentials).
 */
class Operator(
    private val s3: S3Client,
    private val lambda: LambdaClient,
    private val dynamoDb: DynamoDbClient,
    private val stage: String = "local",
    private val region: String = "us-east-1",
) {
    private val json = Json { ignoreUnknownKeys = true }

    val faultBucket = "myorg-payments-event-fault-monitor-$stage-$region"
    private val callsTable = "payments-core-checking-egress-gateway-$stage-calls"
    private val ledgerTable = "payments-core-checking-ingress-gateway-$stage-ledger"

    /** The fault bucket's prefix for one day, without the region (the resubmit tool adds it). */
    fun dayPrefix(daysAgo: Int = 0): String {
        val day = (Clock.System.now() - kotlin.time.Duration.parse("${daysAgo}d")).toLocalDateTime(TimeZone.UTC)
        return "%04d/%02d/%02d/".format(day.year, day.monthNumber, day.dayOfMonth)
    }

    /** Every fault the fault monitor stored for the day. */
    suspend fun faults(prefix: String = dayPrefix()): List<StoredFault> {
        val keys = s3.listObjectsV2(ListObjectsV2Request { bucket = faultBucket; this.prefix = "$region/$prefix" })
            .contents.orEmpty().mapNotNull { it.key }
        return keys.flatMap { key ->
            val text = s3.getObject(GetObjectRequest { bucket = faultBucket; this.key = key }) { it.body?.decodeToString() ?: "" }
            text.lineSequence().filter { it.isNotBlank() }.map { line ->
                val envelope = json.parseToJsonElement(line).jsonObject
                val detail = envelope["detail"]!!.jsonObject
                val tags = detail["tags"]?.jsonObject
                StoredFault(
                    time = envelope["time"]?.jsonPrimitive?.content ?: "",
                    functionName = tags?.get("functionname")?.jsonPrimitive?.content ?: "?",
                    pipeline = tags?.get("pipeline")?.jsonPrimitive?.content ?: "?",
                    error = (detail["err"] as? JsonObject)?.get("message")?.jsonPrimitive?.content ?: "?",
                    objectKey = key,
                )
            }.toList()
        }.sortedBy { it.time }
    }

    /**
     * The fix for a core checking request in doubt: the operator has checked core checking. If it shows the posting,
     * the request was applied after all, so mark it sent (nothing to resubmit). If it doesn't, release the claim, so
     * a resubmission calls core checking again. Returns what it did.
     */
    suspend fun releaseClaim(paymentId: String, txnType: String = "PMT"): String {
        val key = mapOf("pk" to AttributeValue.S("$paymentId#$txnType"))
        val claim = dynamoDb.getItem(GetItemRequest { tableName = callsTable; this.key = key; consistentRead = true }).item
            ?: return "$paymentId#$txnType has no claim: nothing to release"
        val state = (claim["state"] as? AttributeValue.S)?.value

        val postings = dynamoDb.query(QueryRequest {
            tableName = ledgerTable
            indexName = "byReference"
            keyConditionExpression = "REF_NO = :r AND TXN_TYP = :t"
            expressionAttributeValues = mapOf(":r" to AttributeValue.S(paymentId), ":t" to AttributeValue.S(txnType))
        }).count

        if (postings > 0) {
            dynamoDb.updateItem(UpdateItemRequest {
                tableName = callsTable
                this.key = key
                updateExpression = "SET #s = :s"
                expressionAttributeNames = mapOf("#s" to "state")
                expressionAttributeValues = mapOf(":s" to AttributeValue.S("SENT"))
            })
            return "$paymentId#$txnType ($state): core checking shows $postings posting(s); marked SENT, don't resubmit a debit"
        }
        dynamoDb.deleteItem(DeleteItemRequest { tableName = callsTable; this.key = key })
        return "$paymentId#$txnType ($state): core checking shows no posting; claim released, safe to resubmit"
    }

    /** The outcome of a resubmission: how many faults were resubmitted, and which still failed. */
    data class Resubmitted(val total: Int, val failed: List<String>) {
        val ok get() = failed.isEmpty()
    }

    /** Whether core checking's breaker is closed: part of "fix first" before resubmitting core checking faults. */
    suspend fun coreCheckingIsUp(): Boolean {
        val item = dynamoDb.getItem(GetItemRequest {
            tableName = callsTable
            key = mapOf("pk" to AttributeValue.S("BREAKER#core-checking"))
            consistentRead = true
        }).item ?: return true
        return (item["state"] as? AttributeValue.S)?.value == "CLOSED"
    }

    /**
     * Resubmits the day's faults of [functionName] to it, with their original records, and checks every outcome.
     * A resubmission is a direct invocation: nothing retries it if it fails, and a function that throws still answers
     * 200 (the failure is in `FunctionError`). So each one is checked, retried with back-off, and reported if it
     * still fails.
     */
    suspend fun resubmit(
        functionName: String,
        prefix: String = dayPrefix(),
        dry: Boolean = false,
        attempts: Int = 5,
        firstBackoff: kotlin.time.Duration = kotlin.time.Duration.parse("2s"),
    ): Resubmitted {
        val tool = ResubmitFaults()
        val args = ResubmitFaults.Args(bucket = faultBucket, region = region, prefix = prefix, functionname = functionName, dry = dry)
        val requests = tool.filterAndPrepareRequests(args, s3).mapNotNull { it.invokeRequest }
        if (dry) return Resubmitted(requests.size, emptyList())

        val failed = mutableListOf<String>()
        requests.forEachIndexed { index, request ->
            var backoff = firstBackoff
            var error: String? = null
            for (attempt in 1..attempts) {
                val response = lambda.invoke(request)
                error = response.functionError?.let { "$it: ${response.payload?.decodeToString()?.take(300)}" }
                if (error == null) break
                if (attempt < attempts) {
                    kotlinx.coroutines.delay(backoff)
                    backoff *= 2
                }
            }
            if (error != null) failed += "resubmission ${index + 1} of ${requests.size} still fails: $error"
        }
        return Resubmitted(requests.size, failed)
    }

    companion object {
        /** Clients for LocalStack, unless AWS_ENDPOINT_URL says otherwise ("aws" for the real thing). */
        fun local(): Operator {
            val endpoint = System.getenv("AWS_ENDPOINT_URL") ?: "http://localhost:4566"
            val region = System.getenv("AWS_REGION") ?: "us-east-1"
            val credentials = StaticCredentialsProvider {
                accessKeyId = System.getenv("AWS_ACCESS_KEY_ID") ?: "test"
                secretAccessKey = System.getenv("AWS_SECRET_ACCESS_KEY") ?: "test"
            }
            val url = Url.parse(endpoint)
            return Operator(
                s3 = S3Client { this.region = region; endpointUrl = url; credentialsProvider = credentials; forcePathStyle = true },
                lambda = LambdaClient { this.region = region; endpointUrl = url; credentialsProvider = credentials },
                dynamoDb = DynamoDbClient { this.region = region; endpointUrl = url; credentialsProvider = credentials },
                stage = System.getenv("STAGE") ?: "local",
                region = region,
            )
        }
    }
}

private fun usage(): Nothing {
    println(
        """
        Usage: operator.sh <command>
          faults                               the faults the fault monitor stored today
          release-claim <payment> [PMT|RFND]   after checking core checking: mark sent, or release for resubmission
          resubmit <function> [--dry-run]      resubmit today's faults of a function with their original records,
                                               checking every outcome (exit 1 if any still fails)
        """.trimIndent()
    )
    exitProcess(1)
}

fun main(args: Array<String>) = runBlocking {
    val operator = Operator.local()
    when (args.firstOrNull()) {
        "faults" -> {
            val faults = operator.faults()
            if (faults.isEmpty()) println("No faults today in ${operator.faultBucket}.")
            faults.forEach { println("${it.time}  ${it.functionName} [${it.pipeline}]\n    ${it.error.lineSequence().first()}") }
        }
        "release-claim" -> println(operator.releaseClaim(args.getOrNull(1) ?: usage(), args.getOrNull(2) ?: "PMT"))
        "resubmit" -> {
            val function = args.getOrNull(1) ?: usage()
            if ("core-checking" in function && !operator.coreCheckingIsUp() && "--force" !in args) {
                println("Core checking's breaker isn't closed: fix first. Resubmit once it is back (or pass --force).")
                exitProcess(2)
            }
            val result = operator.resubmit(function, dry = "--dry-run" in args)
            if ("--dry-run" in args) println("Would resubmit ${result.total} fault(s) to $function.")
            else println("Resubmitted ${result.total} fault(s) to $function; ${result.failed.size} still failing.")
            result.failed.forEach { println("  $it") }
            if (!result.ok) exitProcess(1)
        }
        else -> usage()
    }
}
