package org.myorg.sut

import aws.sdk.kotlin.services.sns.SnsClient
import aws.sdk.kotlin.services.sns.model.PublishRequest
import com.amazonaws.services.lambda.runtime.Context
import com.amazonaws.services.lambda.runtime.RequestHandler
import com.amazonaws.services.lambda.runtime.events.KinesisFirehoseEvent
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.*
import mu.KotlinLogging
import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets
import java.time.Instant
import java.time.ZoneId
import java.util.*
import java.util.zip.GZIPInputStream

class Transform : RequestHandler<KinesisFirehoseEvent, FirehoseTransformResponse> {

    private val logger = KotlinLogging.logger {}

    override fun handleRequest(
        input: KinesisFirehoseEvent,
        context: Context,
    ): FirehoseTransformResponse = runBlocking {
        val notifications = linkedMapOf<String, Notification>()

        val results = input.records.map { record ->
            val uow = TransformUnitOfWork(
                recordId = record.recordId,
                originalData = StandardCharsets.UTF_8.decode(record.data).toString(),
                ctx = context,
                notifications = notifications,
            )

            tryCatch(::unbase64Data)(uow)
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

        sendNotifications(notifications)

        FirehoseTransformResponse(records = results)
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
        val detail = event["detail"] as? JsonObject ?: return uow
        val tags = detail["tags"] as? JsonObject ?: JsonObject(emptyMap())
        val err = detail["err"] as? JsonObject ?: JsonObject(emptyMap())

        val timestamp = detail.longOrNull("timestamp") ?: 0L

        val d = Instant.ofEpochMilli(timestamp)
            .atZone(ZoneId.systemDefault())

        val t = "${d.year}${d.monthValue - 1}${d.dayOfMonth}${d.hour}"

        val account = tags.stringOrNull("account") ?: "undefined"
        val region = tags.stringOrNull("region") ?: "undefined"
        val functionName = tags.stringOrNull("functionname") ?: "undefined"
        val pipeline = tags.stringOrNull("pipeline") ?: "undefined"
        val errorMessage = err.stringOrNull("message") ?: ""

        val messageDeduplicationId = "$functionName$pipeline$t$errorMessage"
            .take(128)

        val subject = "Fault: $account,$region,$functionName,$pipeline"
            .take(100)

        val fault = prettyJson.encodeToString(JsonElement.serializer(), event)
        val error = prettyJson.encodeToString(JsonElement.serializer(), err)

        val message = if (fault.length < 1024 * 256) {
            fault
        } else {
            error
        }

        uow.notifications[messageDeduplicationId] = Notification(
            subject = subject,
            messageDeduplicationId = messageDeduplicationId,
            messageGroupId = subject,
            message = message,
        )

        return uow
    }

    private suspend fun sendNotifications(
        notifications: Map<String, Notification>,
    ) {
        if (notifications.isEmpty()) {
            return
        }

        val topicArn = System.getenv("TOPIC_ARN")

        SnsClient {
            region = System.getenv("AWS_REGION") ?: "us-east-1"
        }.use { sns ->
            notifications.values.forEach { notification ->
                try {
                    val response = sns.publish(
                        PublishRequest {
                            this.topicArn = topicArn
                            this.subject = notification.subject
                            this.message = notification.message
                            this.messageDeduplicationId = notification.messageDeduplicationId
                            this.messageGroupId = notification.messageGroupId
                        }
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