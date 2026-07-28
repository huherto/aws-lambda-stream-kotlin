package io.github.huherto.awsLambdaStream.tools

import aws.sdk.kotlin.services.lambda.LambdaClient
import aws.sdk.kotlin.services.lambda.model.InvocationType
import aws.sdk.kotlin.services.lambda.model.InvokeRequest
import aws.sdk.kotlin.services.s3.S3Client
import aws.sdk.kotlin.services.s3.model.GetObjectRequest
import aws.sdk.kotlin.services.s3.model.ListObjectsV2Request
import aws.smithy.kotlin.runtime.content.toByteArray
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import kotlinx.datetime.TimeZone
import kotlinx.datetime.number
import kotlinx.datetime.toLocalDateTime
import kotlinx.serialization.json.*
import java.io.File
import kotlin.math.floor
import kotlin.time.Clock
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.ExperimentalTime

class ReplayEvents {

    private val json = Json {
        ignoreUnknownKeys = true
        prettyPrint = true
    }

    data class Args(
        val bucket: String? = null,
        val region: String? = null,
        val prefix: String,
        val type: String = "*",
        val functionname: String? = null,
        val qualifier: String = "\$LATEST",
        val dry: Boolean = false,
        val async: Boolean = false,
        val continuationToken: String? = null,
        val batch: Int = 25,
        val parallel: Int = 16,
        val batchTimeout: Long = 5_000,
        val rate: Int = 3,
        val window: Long = 500,
    )

    data class BatchCounters(
        var max: Int = 0,
        var timeout: Int = 0,
    )

    data class InvokedCounters(
        var total: Int = 0,
        var statuses: MutableMap<Int, Int> = mutableMapOf(),
    )

    data class Counters(
        var list: Int = 0,
        var get: Int = 0,
        var events: Int = 0,
        var match: Int = 0,
        var recordCount: Int = 0,
        var batch: BatchCounters = BatchCounters(),
        var types: MutableMap<String, Int> = mutableMapOf(),
        var functions: MutableMap<String, Int> = mutableMapOf(),
        var invoked: InvokedCounters? = null,
        var errors: Int = 0,
        var errored: MutableList<UnitOfWork> = mutableListOf(),
    )

    data class ListedObject(
        val key: String,
        val isTruncated: Boolean,
        val nextContinuationToken: String?,
    )

    data class UnitOfWork(
        val argv: Args? = null,
        val listRequest: ListObjectsV2Request? = null,
        val listResponse: ListedObject? = null,
        val getRequest: GetObjectRequest? = null,
        val getResponseLine: String? = null,
        val record: JsonObject? = null,
        val event: JsonObject? = null,
        val batch: List<UnitOfWork>? = null,
        val recordCount: Int? = null,
        val invokeRequest: InvokeRequest? = null,
        val invokeResponseStatusCode: Int? = null,
        val err: Throwable? = null,
    )

    private val start = System.currentTimeMillis()
    private val counterLock = Any()

    private val maxPayloadBytes = 100_000

    val counters = Counters()

    fun runtimeMinutes(): Double {
        return (System.currentTimeMillis() - start).toDouble() / 1000.0 / 60.0
    }

    suspend fun main(
        s3: S3Client,
        lambda: LambdaClient,
    ) {
        val argv = loadArgs()

        printJson(argv)

        head(argv, s3)
            .filter(filterByType(argv))
            .onEach { count(counters, it) }
            .batchWithSize(
                maxBytes = maxPayloadBytes,
                timeoutMillis = argv.batchTimeout,
            )
            .map(::toBatchUow)
            .map { withInvokeRequest(it, argv) }
            .rateLimit(
                rate = argv.rate,
                windowMillis = argv.window,
            )
            .mapParallel(argv.parallel) { invokeLambda(lambda, it) }
            .onEach(::debug)
            .collect { uow ->
                count(counters, uow)
            }

        println("======================================")
        println("Running time (minutes): ${runtimeMinutes()}")
        println("Gap: ${counters.list - counters.get}")
        println("Final Counters:")
        printJson(counters)
        println("======================================")
    }

    @OptIn(ExperimentalTime::class)
    fun loadArgs(): Args {
        val now = Clock.System.now().toLocalDateTime(TimeZone.UTC)
        val defaultPrefix = "%04d/%02d/%02d/".format(
            now.year,
            now.month.number,
            now.day,
        )

        val config = findUpConfigFile()
            ?.readText()
            ?.takeIf { it.isNotBlank() }
            ?.let { json.parseToJsonElement(it).jsonObject }
            ?: JsonObject(emptyMap())

        return Args(
            bucket = config.stringValue("bucket") ?: System.getenv("BUCKET_NAME"),
            region = config.stringValue("region") ?: System.getenv("AWS_REGION"),
            prefix = System.getenv("PREFIX")
                ?: config.stringValue("prefix")
                ?: defaultPrefix,
            type = System.getenv("TYPE")
                ?: config.stringValue("type")
                ?: "*",
            functionname = System.getenv("FUNCTION_NAME")
                ?: config.stringValue("functionname"),
            qualifier = config.stringValue("qualifier") ?: "\$LATEST",
            dry = System.getenv("DRY_RUN") == "true" ||
                    config.booleanValue("dry") == true,
            async = config.booleanValue("async") == true,
            continuationToken = System.getenv("MARKER")
                ?: config.stringValue("continuationToken"),
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

    private fun findUpConfigFile(): File? {
        var dir: File? = File(System.getProperty("user.dir"))

        while (dir != null) {
            val eventSrc = File(dir, ".eventsrc")
            val eventSrcJson = File(dir, ".eventsrc.json")

            if (eventSrc.exists()) return eventSrc
            if (eventSrcJson.exists()) return eventSrcJson

            dir = dir.parentFile
        }

        return null
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    fun head(
        argv: Args,
        s3: S3Client,
    ): Flow<UnitOfWork> {
        val bucket = argv.bucket
            ?: error("Missing S3 bucket. Set bucket in config or BUCKET_NAME.")

        val initialUows = argv.prefix
            .split(",")
            .map { it.trim() }
            .filter { it.isNotBlank() }
            .map { prefix ->
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
                    },
                )
            }

        return pageObjectsFromS3(
            s3 = s3,
            uows = initialUows,
            parallel = 1,
        )
            .mapParallel(argv.parallel) { uow ->
                val key = uow.listResponse?.key
                    ?: error("Missing listed object key")

                getObjectFromS3(
                    s3 = s3,
                    uow = uow.copy(
                        getRequest = GetObjectRequest {
                            this.bucket = bucket
                            this.key = key
                        },
                    ),
                )
            }
            .flatMapConcat { splitLines(it).asFlow() }
            .map { parseEventBridgeLine(it) }
            .onEach(::debug)
    }

    private fun filterByType(argv: Args): (UnitOfWork) -> Boolean {
        return { uow ->
            val eventType = uow.event?.string("type")

            when {
                eventType == null -> false

                argv.type.startsWith("regex:") -> {
                    val regex = Regex(argv.type.substring(6))
                    regex.containsMatchIn(eventType)
                }

                argv.type == "*" -> true

                argv.type.endsWith("*") -> {
                    val prefix = argv.type.substring(0, argv.type.length - 1)
                    println("prefix: $prefix ${eventType.startsWith(prefix)} $eventType")
                    eventType.startsWith(prefix)
                }

                else -> argv.type == eventType
            }
        }
    }

    private fun parseEventBridgeLine(uow: UnitOfWork): UnitOfWork {
        val line = uow.getResponseLine ?: return uow

        return try {
            val eventBridgeEvent = json.parseToJsonElement(line).jsonObject
            val detail = eventBridgeEvent["detail"]?.jsonObject
                ?: error("Missing detail field")

            val envelope = JsonObject(
                eventBridgeEvent.filterKeys { it != "detail" },
            )

            synchronized(counterLock) {
                counters.events += 1
            }

            uow.copy(
                record = JsonObject(
                    mapOf("eb" to envelope),
                ),
                event = detail,
            )
        } catch (error: Throwable) {
            errors(error, uow)
        }
    }

    private fun toBatchUow(batch: List<UnitOfWork>): UnitOfWork {
        return UnitOfWork(batch = batch)
    }

    private fun withInvokeRequest(
        uow: UnitOfWork,
        argv: Args,
    ): UnitOfWork {
        val records = if (uow.batch != null) {
            uow.batch.mapNotNull { it.event }
        } else {
            listOfNotNull(uow.event)
        }

        val payloadJson = toKinesisRecords(records)
        val payloadBytes = json.encodeToString(JsonElement.serializer(), payloadJson)
            .encodeToByteArray()

        val invocationType =
            if (argv.dry) {
                InvocationType.DryRun
            } else if (argv.async && payloadBytes.size <= maxPayloadBytes) {
                InvocationType.Event
            } else {
                InvocationType.RequestResponse
            }

        return uow.copy(
            recordCount = uow.batch?.size ?: 1,
            invokeRequest = InvokeRequest {
                this.functionName = argv.functionname
                this.qualifier = argv.qualifier
                this.invocationType = invocationType
                this.payload = payloadBytes
            },
        )
    }

    private fun toKinesisRecords(events: List<JsonObject>): JsonObject {
        return JsonObject(
            mapOf(
                "Records" to JsonArray(
                    events.map { event ->
                        JsonObject(
                            mapOf(
                                "kinesis" to JsonObject(
                                    mapOf(
                                        "data" to JsonPrimitive(
                                            json.encodeToString(JsonElement.serializer(), event),
                                        ),
                                    ),
                                ),
                                "eventSource" to JsonPrimitive("aws:kinesis"),
                                "eventName" to JsonPrimitive("aws:kinesis:record"),
                            ),
                        )
                    },
                ),
            ),
        )
    }

    private suspend fun invokeLambda(
        lambda: LambdaClient,
        uow: UnitOfWork,
    ): UnitOfWork {
        val request = uow.invokeRequest ?: return uow

        return try {
            val response = lambda.invoke(request)

            uow.copy(
                invokeResponseStatusCode = response.statusCode,
            )
        } catch (error: Throwable) {
            errors(error, uow)
        }
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
        synchronized(counterLock) {
            val event = uow.event

            if (event != null && event["type"] != null) {
                val type = event.string("type") ?: "unknown"
                val tags = event.jsonObjectOrNull("tags")

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
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    private fun pageObjectsFromS3(
        s3: S3Client,
        uows: List<UnitOfWork>,
        parallel: Int,
    ): Flow<UnitOfWork> {
        return uows
            .asFlow()
            .mapParallel(parallel) { listObjectsForPrefix(s3, it) }
            .flatMapConcat { it.asFlow() }
    }

    private suspend fun listObjectsForPrefix(
        s3: S3Client,
        initialUow: UnitOfWork,
    ): List<UnitOfWork> {
        val results = mutableListOf<UnitOfWork>()
        var continuationToken = initialUow.listRequest?.continuationToken

        do {
            val original = initialUow.listRequest
                ?: error("Missing list request")

            val configuredParallel = initialUow.argv?.parallel ?: 16

            val maxKeys =
                if (floor(runtimeMinutes()).toInt() > 22) {
                    2
                } else {
                    (configuredParallel - 5).coerceAtLeast(1)
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

                synchronized(counterLock) {
                    counters.list += contents.size
                }

                println("======================================")
                println("Prefix: ${request.prefix}")
                println("ContinuationToken: $continuationToken")
                println("Contents: ${contents.size}")
                println("Running time (minutes): ${runtimeMinutes()}")
                println("Gap: ${counters.list - counters.get}")
                println("Counters:")
                printJson(counters)
                println("======================================")

                contents.forEach { obj ->
                    val key = obj.key ?: return@forEach

                    results += initialUow.copy(
                        listRequest = request,
                        listResponse = ListedObject(
                            key = key,
                            isTruncated = response.isTruncated == true,
                            nextContinuationToken = response.nextContinuationToken,
                        ),
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
    ): UnitOfWork {
        val request = uow.getRequest
            ?: error("Missing get request")

        synchronized(counterLock) {
            counters.get += 1
        }

        println("Get: ${request.key}")

        return try {
            val text = s3.getObject(request) { response ->
                response.body?.toByteArray()?.decodeToString().orEmpty()
            }

            uow.copy(getResponseLine = text)
        } catch (error: Throwable) {
            errors(error, uow)
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

    private fun Flow<UnitOfWork>.batchWithSize(
        maxBytes: Int,
        timeoutMillis: Long,
    ): Flow<List<UnitOfWork>> = flow {
        val input = this@batchWithSize
        val output = Channel<List<UnitOfWork>>(Channel.UNLIMITED)

        val batched = mutableListOf<UnitOfWork>()
        var timeoutJob: Job? = null

        suspend fun flush() {
            if (batched.isNotEmpty()) {
                output.send(batched.toList())
                batched.clear()
            }

            timeoutJob?.cancel()
            timeoutJob = null
        }

        coroutineScope {
            launch {
                input.collect { item ->
                    val candidate = batched + item
                    val size = batchPayloadSize(candidate)

                    if (size <= maxBytes) {
                        batched += item
                    } else {
                        println("**** BATCH MAX ****")
                        synchronized(counterLock) {
                            counters.batch.max += 1
                        }

                        flush()
                        batched += item
                    }

                    if (batched.size == 1 && timeoutJob == null) {
                        timeoutJob = launch {
                            delay(timeoutMillis.milliseconds)
                            println("**** BATCH TIMEOUT ****")

                            synchronized(counterLock) {
                                counters.batch.timeout += 1
                            }

                            flush()
                        }
                    }
                }

                flush()
                output.close()
            }

            for (batch in output) {
                emit(batch)
            }
        }
    }

    private fun batchPayloadSize(batch: List<UnitOfWork>): Int {
        val payload = JsonObject(
            mapOf(
                "Records" to JsonArray(
                    batch.mapNotNull { it.event },
                ),
            ),
        )

        return json.encodeToString(JsonElement.serializer(), payload)
            .encodeToByteArray()
            .size
    }

    private fun <T> Flow<T>.rateLimit(
        rate: Int,
        windowMillis: Long,
    ): Flow<T> = flow {
        val safeRate = rate.coerceAtLeast(1)
        val safeWindowMillis = windowMillis.coerceAtLeast(0)
        var emittedInWindow = 0

        collect { value ->
            if (emittedInWindow == safeRate) {
                delay(safeWindowMillis.milliseconds)
                emittedInWindow = 0
            }

            emit(value)
            emittedInWindow += 1
        }
    }

    private fun <T, R> Flow<T>.mapParallel(
        parallelism: Int,
        transform: suspend (T) -> R,
    ): Flow<R> = channelFlow {
        val safeParallelism = parallelism.coerceAtLeast(1)
        val semaphore = Semaphore(safeParallelism)

        collect { value ->
            launch {
                semaphore.withPermit {
                    send(transform(value))
                }
            }
        }
    }.buffer(parallelism.coerceAtLeast(1))

    private fun debug(data: Any?) {
        if (System.getenv("DEBUG")?.contains("cli") == true) {
            printJson(data)
        }
    }

    private fun printJson(data: Any?) {
        println(
            when (data) {
                null -> "null"
                is Args -> data.toString()
                is Counters -> data.toString()
                else -> data.toString()
            },
        )
    }
}


private fun JsonObject.jsonObjectOrNull(name: String): JsonObject? {
    return this[name] as? JsonObject
}