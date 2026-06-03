package org.myorg.sut

import aws.sdk.kotlin.services.lambda.LambdaClient
import aws.sdk.kotlin.services.lambda.model.InvocationType
import aws.sdk.kotlin.services.lambda.model.InvokeRequest
import aws.sdk.kotlin.services.s3.S3Client
import aws.sdk.kotlin.services.s3.model.GetObjectRequest
import aws.sdk.kotlin.services.s3.model.ListObjectsV2Request
import aws.smithy.kotlin.runtime.content.toByteArray
import kotlinx.coroutines.*
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import kotlinx.datetime.number
import kotlinx.datetime.toLocalDateTime
import kotlinx.serialization.json.*
import java.io.File
import kotlin.math.floor
import kotlin.time.Clock
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.ExperimentalTime

private val json = Json {
    ignoreUnknownKeys = true
    prettyPrint = true
}

data class Args(
    val bucket: String? = null,
    val region: String? = null,
    val prefix: String,
    val functionname: String? = null,
    val qualifier: String? = null,
    val dry: Boolean = false,
    val async: Boolean = false,
    val continuationToken: String? = null,
    val batch: Int = 25,
    val parallel: Int = 16,
    val batchTimeout: Long = 5_000,
    val rate: Int = 3,
    val window: Long = 500,
)

data class Counters(
    var list: Int = 0,
    var get: Int = 0,
    var events: Int = 0,
    var match: Int = 0,
    var recordCount: Int = 0,
    var types: MutableMap<String, Int> = mutableMapOf(),
    var functions: MutableMap<String, Int> = mutableMapOf(),
    var invoked: InvokedCounters? = null,
    var errors: Int = 0,
    var errored: MutableList<UnitOfWork> = mutableListOf(),
)

data class InvokedCounters(
    var total: Int = 0,
    var statuses: MutableMap<Int, Int> = mutableMapOf(),
)

data class UnitOfWork(
    val argv: Args? = null,
    val listRequest: ListObjectsV2Request? = null,
    val listResponse: ListedObject? = null,
    val getRequest: GetObjectRequest? = null,
    val getResponseLine: String? = null,
    val record: JsonObject? = null,
    val event: JsonObject? = null,
    val recordCount: Int? = null,
    val invokeRequest: InvokeRequest? = null,
    val invokeResponseStatusCode: Int? = null,
    val err: Throwable? = null,
)

data class ListedObject(
    val key: String,
    val isTruncated: Boolean,
    val nextContinuationToken: String?,
)

private val start = System.currentTimeMillis()
private val counters = Counters()

private fun runtimeMinutes(): Double {
    return (System.currentTimeMillis() - start).toDouble() / 1000.0 / 60.0
}

private fun debug(data: Any?) {
    if (System.getenv("DEBUG")?.contains("cli") == true) {
        println(toPrettyString(data))
    }
}

private fun print(data: Any?) {
    println(toPrettyString(data))
}

private fun toPrettyString(data: Any?): String {
    return when (data) {
        null -> "null"
        is Args -> json.encodeToString(
            kotlinx.serialization.serializer(),
            mapOf(
                "bucket" to data.bucket,
                "region" to data.region,
                "prefix" to data.prefix,
                "functionname" to data.functionname,
                "qualifier" to data.qualifier,
                "dry" to data.dry.toString(),
                "async" to data.async.toString(),
                "continuationToken" to data.continuationToken,
                "batch" to data.batch.toString(),
                "parallel" to data.parallel.toString(),
                "batchTimeout" to data.batchTimeout.toString(),
                "rate" to data.rate.toString(),
                "window" to data.window.toString(),
            )
        )

        is Counters -> """
            {
              "list": ${data.list},
              "get": ${data.get},
              "events": ${data.events},
              "match": ${data.match},
              "recordCount": ${data.recordCount},
              "types": ${data.types},
              "functions": ${data.functions},
              "invoked": ${data.invoked},
              "errors": ${data.errors}
            }
        """.trimIndent()

        else -> data.toString()
    }
}

private fun findUpConfigFile(): File? {
    var dir: File? = File(System.getProperty("user.dir"))

    while (dir != null) {
        val faultSrc = File(dir, ".faultsrc")
        val faultSrcJson = File(dir, ".faultsrc.json")

        if (faultSrc.exists()) return faultSrc
        if (faultSrcJson.exists()) return faultSrcJson

        dir = dir.parentFile
    }

    return null
}

private fun JsonObject.stringValue(name: String): String? {
    return this[name]?.let {
        when (it) {
            is JsonPrimitive -> it.contentOrNull
            else -> null
        }
    }
}

private fun JsonObject.booleanValue(name: String): Boolean? {
    return this[name]?.let {
        when (it) {
            is JsonPrimitive -> it.booleanOrNull
            else -> null
        }
    }
}

private fun JsonObject.intValue(name: String): Int? {
    return this[name]?.let {
        when (it) {
            is JsonPrimitive -> it.intOrNull
            else -> null
        }
    }
}

@OptIn(ExperimentalTime::class)
private fun loadArgs(): Args {
    val now = Clock.System.now().toLocalDateTime(kotlinx.datetime.TimeZone.UTC)
    val defaultPrefix = "%04d/%02d/%02d/".format(now.year, now.month.number, now.day)
    val config = findUpConfigFile()
        ?.readText()
        ?.takeIf { it.isNotBlank() }
        ?.let { json.parseToJsonElement(it).jsonObject }
        ?: JsonObject(emptyMap())

    return Args(
        bucket = config.stringValue("bucket") ?: System.getenv("BUCKET_NAME"),
        region = config.stringValue("region") ?: System.getenv("AWS_REGION"),
        prefix = System.getenv("PREFIX") ?: config.stringValue("prefix") ?: defaultPrefix,
        functionname = System.getenv("FUNCTION_NAME") ?: config.stringValue("functionname"),
        qualifier = config.stringValue("qualifier"),
        dry = System.getenv("DRY_RUN") == "true" || config.booleanValue("dry") == true,
        async = config.booleanValue("async") == true,
        continuationToken = System.getenv("MARKER") ?: config.stringValue("continuationToken"),
        batch = config.intValue("batch") ?: 25,
        parallel = config.intValue("parallel") ?: 16,
        batchTimeout = System.getenv("BATCH_TIMEOUT")?.toLongOrNull()
            ?: config.intValue("batchTimeout")?.toLong()
            ?: 5_000,
        rate = System.getenv("RATE")?.toIntOrNull()
            ?: config.intValue("rate")
            ?: 3,
        window = config.intValue("window")?.toLong() ?: 500,
    )
}

private suspend fun main() {
    val argv = loadArgs()

    print(argv)

    LambdaClient {
        region = argv.region ?: System.getenv("AWS_REGION")
    }.use { lambda ->
        S3Client {
            region = argv.region ?: System.getenv("AWS_REGION")
        }.use { s3 ->
            val processed = head(argv, s3)
                .asSequence()
                .filter(filterByFunctionName(argv))
                .filter(::hasRecord)
                .map { withInvokeRequest(it, argv) }
                .toList()

            val invoked = invokeLambdaRateLimited(
                lambda = lambda,
                uows = processed,
                rate = argv.rate,
                windowMillis = argv.window,
                parallel = argv.parallel,
            )

            invoked.forEach { count(counters, it) }
        }
    }

    println("======================================")
    println("Running time (minutes): ${runtimeMinutes()}")
    println("Gap: ${counters.list - counters.get}")
    println("Final Counters:")
    print(counters)
    println("======================================")
}

private fun filterByFunctionName(argv: Args): (UnitOfWork) -> Boolean {
    return { uow ->
        if (argv.functionname == "*") {
            true
        } else {
            argv.functionname == uow.event
                ?.jsonObject("tags")
                ?.string("functionname")
        }
    }
}

private fun hasRecord(uow: UnitOfWork): Boolean {
    val uowJson = uow.event?.jsonObject("uow") ?: return false

    if (uowJson["record"] != null) {
        return true
    }

    val batch = uowJson["batch"] as? JsonArray ?: return false
    val first = batch.firstOrNull()?.jsonObject ?: return false

    return first["record"] != null
}

private fun withInvokeRequest(
    uow: UnitOfWork,
    argv: Args,
): UnitOfWork {
    val eventUow = uow.event?.jsonObject("uow")
        ?: error("Missing event.uow")

    val batch = eventUow["batch"] as? JsonArray

    val records: List<JsonElement> =
        if (batch != null) {
            batch.mapNotNull { item ->
                item.jsonObject["record"]
            }
        } else {
            listOfNotNull(eventUow["record"])
        }

    val payloadJson = JsonObject(
        mapOf(
            "Records" to JsonArray(records)
        )
    )

    val payloadBytes = json.encodeToString(JsonElement.serializer(), payloadJson)
        .encodeToByteArray()

    val invocationType =
        if (argv.dry) {
            InvocationType.DryRun
        } else if (argv.async && payloadBytes.size <= 100_000) {
            InvocationType.Event
        } else {
            InvocationType.RequestResponse
        }

    val functionName = uow.event
        .jsonObject("tags")
        ?.string("functionname")
        ?: error("Missing event.tags.functionname")

    return uow.copy(
        recordCount = batch?.size ?: 1,
        invokeRequest = InvokeRequest {
            this.functionName = functionName
            this.qualifier = argv.qualifier
            this.invocationType = invocationType
            this.payload = payloadBytes
        }
    )
}

@OptIn(DelicateCoroutinesApi::class)
private suspend fun invokeLambdaRateLimited(
    lambda: LambdaClient,
    uows: List<UnitOfWork>,
    rate: Int,
    windowMillis: Long,
    parallel: Int,
): List<UnitOfWork> {
    val semaphore = Semaphore(parallel)
    val chunks = uows.chunked(rate)

    val results = mutableListOf<UnitOfWork>()

    for (chunk in chunks) {
        val invoked = chunk.map { uow ->
            kotlinx.coroutines.GlobalScope.async(Dispatchers.IO) {
                semaphore.withPermit {
                    try {
                        val request = uow.invokeRequest
                            ?: return@withPermit uow

                        val response = lambda.invoke(request)

                        val updated = uow.copy(
                            invokeResponseStatusCode = response.statusCode
                        )

                        debug(updated)

                        updated
                    } catch (error: Throwable) {
                        errors(error, uow)
                    }
                }
            }
        }.awaitAll()

        results += invoked

        delay(windowMillis.milliseconds)
    }

    return results
}

private fun errors(
    error: Throwable,
    uow: UnitOfWork,
): UnitOfWork {
    System.err.println(error.message)

    if (error.message == "The provided token has expired.") {
        throw error
    }

    return uow.copy(err = error)
}

private fun count(
    counters: Counters,
    uow: UnitOfWork,
): Counters {
    val event = uow.event

    if (event != null && event["type"] != null) {
        val type = event.string("type") ?: "unknown"
        val tags = event.jsonObject("tags")
        val functionName = tags?.string("functionname") ?: "unknown"
        val pipeline = "$functionName|${tags?.string("pipeline") ?: "unknown"}"

        counters.match += 1

        counters.types[type] = (counters.types[type] ?: 0) + 1
        counters.functions[pipeline] = (counters.functions[pipeline] ?: 0) + 1
    }

    if (uow.recordCount != null) {
        counters.recordCount += uow.recordCount
    }

    if (uow.invokeRequest != null) {
        if (counters.invoked == null) {
            counters.invoked = InvokedCounters()
        }

        val invoked = counters.invoked!!
        invoked.total += 1

        if (uow.invokeResponseStatusCode != null) {
            val status = uow.invokeResponseStatusCode
            invoked.statuses[status] = (invoked.statuses[status] ?: 0) + 1
        }
    }

    if (uow.err != null) {
        counters.errors += 1
        counters.errored += uow
    }

    return counters
}

@OptIn(DelicateCoroutinesApi::class)
private suspend fun head(
    argv: Args,
    s3: S3Client,
): List<UnitOfWork> {
    val bucket = argv.bucket
        ?: error("Missing bucket. Set bucket in config or BUCKET_NAME environment variable.")

    val prefixes = argv.prefix.split(",")

    val initialUows = prefixes.map { prefix ->
        val fullPrefix =
            if (argv.region != null) {
                "${argv.region}/$prefix"
            } else {
                prefix
            }

        UnitOfWork(
            argv = argv,
            listRequest = ListObjectsV2Request {
                this.bucket = bucket
                this.prefix = fullPrefix
                this.continuationToken = argv.continuationToken
                    ?.takeUnless { it == "" || it == "undefined" || it == "true" }
            }
        )
    }

    val listed = pageObjectsFromS3(
        s3 = s3,
        uows = initialUows,
        parallel = 1,
    )

    val getSemaphore = Semaphore(argv.parallel)

    return listed.map { uow ->
        kotlinx.coroutines.GlobalScope.async(Dispatchers.IO) {
            getSemaphore.withPermit {
                val key = uow.listResponse?.key
                    ?: error("Missing listed object key")

                val getRequest = GetObjectRequest {
                    this.bucket = bucket
                    this.key = key
                }

                getObjectFromS3(
                    s3 = s3,
                    uow = uow.copy(getRequest = getRequest),
                    getRequest = getRequest,
                )
            }
        }
    }.awaitAll()
        .flatMap(::splitLines)
        .map { uow ->
            counters.events += 1

            val line = uow.getResponseLine
                ?: error("Missing S3 object line")

            val parsed = json.parseToJsonElement(line).jsonObject
            val detail = parsed["detail"]?.jsonObject
                ?: error("Missing detail field")

            val eb = JsonObject(parsed.filterKeys { it != "detail" })

            uow.copy(
                record = JsonObject(
                    mapOf(
                        "eb" to eb
                    )
                ),
                event = detail,
            )
        }
        .onEach(::debug)
}

private suspend fun pageObjectsFromS3(
    s3: S3Client,
    uows: List<UnitOfWork>,
    parallel: Int,
): List<UnitOfWork> {
    val semaphore = Semaphore(parallel)

    return uows.map { uow ->
        kotlinx.coroutines.GlobalScope.async(Dispatchers.IO) {
            semaphore.withPermit {
                listAllObjectsForPrefix(s3, uow)
            }
        }
    }.awaitAll().flatten()
}

private suspend fun listAllObjectsForPrefix(
    s3: S3Client,
    initialUow: UnitOfWork,
): List<UnitOfWork> {
    val results = mutableListOf<UnitOfWork>()

    var continuationToken = initialUow.listRequest?.continuationToken

    do {
        val original = initialUow.listRequest
            ?: error("Missing list request")

        val maxKeys =
            if (floor(runtimeMinutes()).toInt() > 22) {
                2
            } else {
                (initialUow.argv?.parallel ?: 16) - 5
            }

        val request = ListObjectsV2Request {
            bucket = original.bucket
            prefix = original.prefix
            this.continuationToken = continuationToken
            this.maxKeys = maxKeys
        }

        try {
            val response = s3.listObjectsV2(request)
            val contents = response.contents ?: emptyList()

            continuationToken =
                if (response.isTruncated == true) {
                    response.nextContinuationToken
                } else {
                    null
                }

            counters.list += contents.size

            println("======================================")
            println("Prefix: ${request.prefix}")
            println("ContinuationToken: $continuationToken")
            println("Contents: ${contents.size}")
            println("Running time (minutes): ${runtimeMinutes()}")
            println("Gap: ${counters.list - counters.get}")
            println("Counters:")
            print(counters)
            println("======================================")

            contents.forEach { obj ->
                val key = obj.key ?: return@forEach

                results += initialUow.copy(
                    listRequest = request,
                    listResponse = ListedObject(
                        key = key,
                        isTruncated = response.isTruncated == true,
                        nextContinuationToken = response.nextContinuationToken,
                    )
                )
            }
        } catch (error: Throwable) {
            results += initialUow.copy(err = error)
            continuationToken = null
        }
    } while (continuationToken != null)

    return results
}

private suspend fun getObjectFromS3(
    s3: S3Client,
    uow: UnitOfWork,
    getRequest: GetObjectRequest,
): UnitOfWork {
    counters.get += 1

    println("Get: ${getRequest.key}")

    return try {
        val text = s3.getObject(getRequest) { response ->
            response.body?.toByteArray()?.decodeToString().orEmpty()
        }

        uow.copy(getResponseLine = text)
    } catch (error: Throwable) {
        uow.copy(err = error)
    }
}

private fun splitLines(uow: UnitOfWork): List<UnitOfWork> {
    val text = uow.getResponseLine ?: return listOf(uow)

    return text
        .lineSequence()
        .filter { it.isNotBlank() }
        .map { line ->
            uow.copy(getResponseLine = line)
        }
        .toList()
}

private fun JsonObject.jsonObject(name: String): JsonObject? {
    return this[name] as? JsonObject
}

private fun JsonObject.string(name: String): String? {
    return (this[name] as? JsonPrimitive)?.contentOrNull
}