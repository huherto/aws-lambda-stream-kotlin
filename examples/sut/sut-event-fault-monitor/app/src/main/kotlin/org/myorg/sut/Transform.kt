package org.myorg.sut

import aws.sdk.kotlin.runtime.auth.credentials.EnvironmentCredentialsProvider
import aws.sdk.kotlin.services.sns.SnsClient
import aws.sdk.kotlin.services.sns.model.PublishRequest
import aws.smithy.kotlin.runtime.net.url.Url
import com.amazonaws.services.lambda.runtime.Context
import com.amazonaws.services.lambda.runtime.RequestHandler
import com.amazonaws.services.lambda.runtime.RequestStreamHandler
import com.amazonaws.services.lambda.runtime.events.KinesisFirehoseEvent
import io.github.huherto.awsLambdaStream.EnvironmentConfig
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.*
import mu.KotlinLogging
import java.io.ByteArrayInputStream
import java.io.InputStream
import java.io.OutputStream
import java.nio.charset.StandardCharsets
import java.time.Instant
import java.time.ZoneId
import java.util.*
import java.util.zip.GZIPInputStream

class Transform2 : RequestStreamHandler {
    private val logger = KotlinLogging.logger {}

    override fun handleRequest(
        input: InputStream,
        output: OutputStream,
        context: Context,
    ) {
        val raw = input.readBytes().toString(Charsets.UTF_8)
        logger.info { "Raw Firehose event: $raw" }

        // parse manually after inspecting shape
    }
}

class Transform : RequestHandler<KinesisFirehoseEvent, FirehoseTransformResponse> {

    private val logger = KotlinLogging.logger {}

    private val envConfig = EnvironmentConfig()

    override fun handleRequest(
        input: KinesisFirehoseEvent,
        context: Context,
    ): FirehoseTransformResponse = runBlocking {
        val notifications = linkedMapOf<String, Notification>()

        val results = input.records.map { record ->
            val originalData = StandardCharsets.UTF_8.decode(record.data).toString()
            fixRecordId(record)
            logger.info { "Original data: $originalData" }
            val event = json.parseToJsonElement(originalData)
            val notification = createNotification(event as JsonObject)
            notification?.let { notifications[it.messageDeduplicationId] = it }
            val outputData = Base64.getEncoder()
                .encodeToString(
                    originalData.toByteArray()
                )
            FirehoseTransformRecord(
                recordId = record.recordId,
                result = "Ok",
                data = outputData,
            )
        }

        sendNotifications(notifications)

        FirehoseTransformResponse(records = results)
    }

    private fun processAsPipeline(
        record: KinesisFirehoseEvent.Record?,
        originalData: String,
        context: Context,
        notifications: LinkedHashMap<String, Notification>
    ): FirehoseTransformRecord {
        val uow = TransformUnitOfWork(
            recordId = record!!.recordId,
            originalData = originalData,
            ctx = context,
            notifications = notifications,
        )

        return tryCatch(::unbase64Data)(uow)
            .let(tryCatch(::parseEvent))
            .let(tryCatch(::decompressEvent))
            .let(tryCatch(::stringifyEvent))
            .let(tryCatch(::base64Data))
            .let(tryCatch(::addNotification))
            .let { processed ->
                FirehoseTransformRecord(
                    recordId = processed.recordId,
                    result = "Ok",
                    data = processed.data ?: processed.originalData,
                )
            }
    }

    private fun isLocalStack(): Boolean = System.getenv("LOCALSTACK_HOSTNAME") != null ||
            System.getenv("AWS_SAM_LOCAL") == "true"

    private fun fixRecordId(record: KinesisFirehoseEvent.Record) {
        if (record.recordId == null) {
            if (isLocalStack()) {
                // Look for an embedded EventBridge event ID within the record payload
                record.recordId = "local-eb-${UUID.randomUUID()}"
            } else {
                throw IllegalArgumentException("Malformed Firehose Record: 'recordId' is missing.")
            }
        }
    }

    private fun tryCatch(
        f: (TransformUnitOfWork) -> TransformUnitOfWork,
        final: Boolean = false,
    ): (TransformUnitOfWork) -> TransformUnitOfWork = { uow ->
        try {
            if (uow.err != null && !final) {
                uow
            } else {
                f(uow)
            }
        } catch (err: Throwable) {
            logger.error(err) { "Failed to transform Firehose record ${uow.recordId}" }

            uow.copy(
                err = err,
                data = uow.originalData,
            )
        }
    }

    private fun unbase64Data(uow: TransformUnitOfWork): TransformUnitOfWork {
        val decoded = Base64.getDecoder()
            .decode(uow.originalData)
            .toString(Charsets.UTF_8)

        return uow.copy(eventAsString = decoded)
    }

    private fun parseEvent(uow: TransformUnitOfWork): TransformUnitOfWork {
        val eventAsString = requireNotNull(uow.eventAsString) {
            "eventAsString is required"
        }

        return uow.copy(
            event = json.parseToJsonElement(eventAsString),
        )
    }

    private fun decompressEvent(uow: TransformUnitOfWork): TransformUnitOfWork {
        val event = requireNotNull(uow.event) {
            "event is required"
        }

        if (event !is JsonObject) {
            return uow
        }

        val detail = event["detail"] ?: return uow

        val decompressed = JsonObject(
            event.toMutableMap().apply {
                put("detail", decompressJson(detail))
            }
        )

        return uow.copy(event = decompressed)
    }

    private fun stringifyEvent(uow: TransformUnitOfWork): TransformUnitOfWork {
        val event = requireNotNull(uow.event) {
            "event is required"
        }

        return uow.copy(
            data = json.encodeToString(JsonElement.serializer(), event),
        )
    }

    private fun base64Data(uow: TransformUnitOfWork): TransformUnitOfWork {
        val data = requireNotNull(uow.data) {
            "data is required"
        }

        val encoded = Base64.getEncoder()
            .encodeToString("$data\n".toByteArray(Charsets.UTF_8))

        return uow.copy(data = encoded)
    }

    private fun addNotification(uow: TransformUnitOfWork): TransformUnitOfWork {
        val event = uow.event as? JsonObject ?: return uow
        val notification = createNotification(event)
        if (notification != null) {
            uow.notifications[notification.messageDeduplicationId] = notification
        }

        return uow
    }

    private fun createNotification(event: JsonObject): Notification? {
        val detail = event["detail"] as? JsonObject ?: return null
        val tags = detail["tags"] as? JsonObject ?: JsonObject(emptyMap())
        val err = detail["err"] as? JsonObject ?: JsonObject(emptyMap())

        val eventId = event.stringOrNull("id") ?: UUID.randomUUID().toString()
        val timestamp = detail.longOrNull("timestamp") ?: 0L

        val d = Instant.ofEpochMilli(timestamp)
            .atZone(ZoneId.systemDefault())

        val t = "${d.year}${d.monthValue - 1}${d.dayOfMonth}${d.hour}"

        val account = tags.stringOrNull("account") ?: "account"
        val region = tags.stringOrNull("region") ?: "region"
        val functionName = tags.stringOrNull("functionname") ?: "functionname"
        val pipeline = tags.stringOrNull("pipeline") ?: "pipeline"
        val errorMessage = err.stringOrNull("message") ?: ""

        val messageDeduplicationId = "$eventId-$functionName-$pipeline-$t-$errorMessage"
            .take(128)

        val subject = "Fault: $account,$region,$functionName,$pipeline"
            .take(100)

        val messageGroupId = "Fault:$account,$region,$functionName,$pipeline"

        val fault = prettyJson.encodeToString(JsonElement.serializer(), event)
        val error = prettyJson.encodeToString(JsonElement.serializer(), err)

        val message = if (fault.length < 1024 * 256) {
            fault
        } else {
            error
        }

        return Notification(
            subject = subject,
            messageDeduplicationId = messageDeduplicationId,
            messageGroupId = messageGroupId,
            message = message,
        )
    }

    private suspend fun sendNotifications(
        notifications: Map<String, Notification>,
    ) {
        if (notifications.isEmpty()) {
            return
        }

        val topicArn = System.getenv("TOPIC_ARN")
        val endpointUrl = envConfig.endPointUrl()
        val region = envConfig.awsRegion()
        SnsClient {
            this.region = region
            this.credentialsProvider = EnvironmentCredentialsProvider()

            // If an endpoint URL is provided (like http://localhost:4566), use it
            endpointUrl?.let { this.endpointUrl = Url.parse(it) }
        }.use { sns ->
            notifications.values.forEach { notification ->
                try {
                    val request = PublishRequest {
                        this.topicArn = topicArn
                        this.subject = notification.subject
                        this.message = notification.message
                        this.messageDeduplicationId = notification.messageDeduplicationId
                        this.messageGroupId = notification.messageGroupId
                    }
                    logger.info { "SNS publish request: $request" }
                    val response = sns.publish(
                        request
                    )

                    logger.info { "SNS publish response: $response" }
                } catch (err: Throwable) {
                    logger.error(err) {
                        "Failed to publish SNS notification ${notification.messageDeduplicationId}"
                    }
                }
            }
        }
    }

    private fun decompressJson(element: JsonElement): JsonElement {
        return when (element) {
            is JsonObject -> {
                buildJsonObject {
                    element.forEach { key, value ->
                        put(key, decompressJson(value))
                    }
                }
            }

            is JsonArray -> {
                buildJsonArray {
                    element.forEach { value ->
                        add(decompressJson(value))
                    }
                }
            }

            is JsonPrimitive -> {
                if (element.isString && element.content.startsWith(COMPRESSION_PREFIX)) {
                    val compressed = element.content.substring(COMPRESSION_PREFIX.length)
                    json.parseToJsonElement(unzip(compressed))
                } else {
                    element
                }
            }

            JsonNull -> JsonNull
        }
    }

    private fun unzip(base64: String): String {
        val compressed = Base64.getDecoder().decode(base64)

        return GZIPInputStream(ByteArrayInputStream(compressed)).use { gzip ->
            gzip.readBytes().toString(Charsets.UTF_8)
        }
    }

    private fun JsonObject.stringOrNull(name: String): String? {
        return this[name]
            ?.jsonPrimitive
            ?.content
    }

    private fun JsonObject.longOrNull(name: String): Long? {
        return this[name]
            ?.jsonPrimitive
            ?.content
            ?.toLongOrNull()
    }

    private data class TransformUnitOfWork(
        val recordId: String,
        val originalData: String,
        val data: String? = null,
        val eventAsString: String? = null,
        val event: JsonElement? = null,
        val ctx: Context,
        val notifications: MutableMap<String, Notification>,
        val err: Throwable? = null,
    )

    private data class Notification(
        val subject: String,
        val messageDeduplicationId: String,
        val messageGroupId: String,
        val message: String,
    )

    companion object {
        private const val COMPRESSION_PREFIX = "COMPRESSED"

        private val json = Json {
            ignoreUnknownKeys = true
            isLenient = true
            encodeDefaults = false
        }

        private val prettyJson = Json {
            ignoreUnknownKeys = true
            isLenient = true
            prettyPrint = true
            encodeDefaults = false
        }
    }
}

@Serializable
data class FirehoseTransformResponse(
    val records: List<FirehoseTransformRecord>,
)

@Serializable
data class FirehoseTransformRecord(
    val recordId: String,
    val result: String,
    val data: String,
)