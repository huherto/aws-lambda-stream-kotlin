package org.myorg.sut

import com.amazonaws.services.lambda.runtime.Context
import io.kotest.matchers.collections.shouldContainExactly
import io.kotest.matchers.maps.shouldContainKey
import io.kotest.matchers.shouldBe
import io.mockk.mockk
import kotlinx.serialization.json.*
import org.junit.jupiter.api.Test
import java.io.ByteArrayOutputStream
import java.lang.reflect.Method
import java.nio.charset.StandardCharsets
import java.time.Instant
import java.time.ZoneId
import java.util.*
import java.util.zip.GZIPOutputStream

class TransformTest {

    private val transform = Transform()
    private val context = mockk<Context>(relaxed = true)
    private val json = Json {
        ignoreUnknownKeys = true
        isLenient = true
        encodeDefaults = false
    }

    @Test
    fun `unbase64Data decodes original data into event string`() {
        // Arrange
        val originalJson = """{"detail":{"message":"hello"}}"""
        val uow = uow(
            originalData = Base64.getEncoder().encodeToString(originalJson.toByteArray(StandardCharsets.UTF_8)),
        )

        // Act
        val result = invokeUowFunction("unbase64Data", uow)

        // Assert
        result.stringProperty("eventAsString") shouldBe originalJson
        result.stringProperty("originalData") shouldBe uow.stringProperty("originalData")
    }

    @Test
    fun `parseEvent parses event string into json element`() {
        // Arrange
        val eventAsString = """{"detail":{"message":"hello"},"source":"test"}"""
        val uow = uow(eventAsString = eventAsString)

        // Act
        val result = invokeUowFunction("parseEvent", uow)

        // Assert
        result.jsonElementProperty("event") shouldBe json.parseToJsonElement(eventAsString)
    }

    @Test
    fun `stringifyEvent serializes json element and base64Data encodes it with trailing newline`() {
        // Arrange
        val event = buildJsonObject {
            put("source", "test")
            put(
                "detail",
                buildJsonObject {
                    put("message", "hello")
                },
            )
        }
        val uow = uow(event = event)

        // Act
        val stringified = invokeUowFunction("stringifyEvent", uow)
        val encoded = invokeUowFunction("base64Data", stringified)

        // Assert
        stringified.stringProperty("data") shouldBe """{"source":"test","detail":{"message":"hello"}}"""

        val decoded = Base64.getDecoder()
            .decode(encoded.stringProperty("data"))
            .toString(StandardCharsets.UTF_8)

        decoded shouldBe """{"source":"test","detail":{"message":"hello"}}""" + "\n"
    }

    @Test
    fun `decompressEvent recursively decompresses compressed values only under detail`() {
        // Arrange
        val compressedObject = compressed("""{"expanded":true,"count":2}""")
        val compressedArray = compressed("""["a","b"]""")
        val compressedStringOutsideDetail = "COMPRESSED${compressed("""{"should":"stay compressed"}""")}"

        val event = buildJsonObject {
            put("id", "event-1")
            put("outside", compressedStringOutsideDetail)
            put(
                "detail",
                buildJsonObject {
                    put("plain", "value")
                    put("object", "COMPRESSED$compressedObject")
                    put(
                        "nested",
                        buildJsonArray {
                            add(JsonPrimitive("COMPRESSED$compressedArray"))
                            add(JsonNull)
                            add(JsonPrimitive(123))
                        },
                    )
                },
            )
        }
        val uow = uow(event = event)

        // Act
        val result = invokeUowFunction("decompressEvent", uow)

        // Assert
        result.jsonElementProperty("event") shouldBe json.parseToJsonElement(
            """
            {
              "id": "event-1",
              "outside": "$compressedStringOutsideDetail",
              "detail": {
                "plain": "value",
                "object": {
                  "expanded": true,
                  "count": 2
                },
                "nested": [
                  ["a", "b"],
                  null,
                  123
                ]
              }
            }
            """.trimIndent(),
        )
    }

    @Test
    fun `decompressEvent leaves non object events and events without detail unchanged`() {
        // Arrange
        val arrayEvent = buildJsonArray {
            add(JsonPrimitive("value"))
        }
        val objectWithoutDetail = buildJsonObject {
            put("source", "test")
        }

        // Act
        val arrayResult = invokeUowFunction("decompressEvent", uow(event = arrayEvent))
        val objectResult = invokeUowFunction("decompressEvent", uow(event = objectWithoutDetail))

        // Assert
        arrayResult.jsonElementProperty("event") shouldBe arrayEvent
        objectResult.jsonElementProperty("event") shouldBe objectWithoutDetail
    }

    @Test
    fun `decompressJson handles compressed primitives arrays objects and nulls`() {
        // Arrange
        val compressedDetail = compressed("""{"inner":["x",null,{"ok":true}]}""")
        val element = buildJsonObject {
            put("compressed", "COMPRESSED$compressedDetail")
            put(
                "array",
                buildJsonArray {
                    add(JsonPrimitive("plain"))
                    add(JsonNull)
                },
            )
        }

        // Act
        val result = invokeJsonFunction("decompressJson", element)

        // Assert
        result shouldBe json.parseToJsonElement(
            """
            {
              "compressed": {
                "inner": [
                  "x",
                  null,
                  {
                    "ok": true
                  }
                ]
              },
              "array": [
                "plain",
                null
              ]
            }
            """.trimIndent(),
        )
    }

    @Test
    fun `unzip decodes gzipped base64 json`() {
        // Arrange
        val original = """{"message":"hello"}"""
        val zipped = compressed(original)

        // Act
        val result = invokeStringFunction("unzip", zipped)

        // Assert
        result shouldBe original
    }

    @Test
    fun `addNotification creates notification from event detail`() {
        // Arrange
        val notifications = linkedMapOf<String, Any>()
        val timestamp = 1_700_000_000_000L
        val event = json.parseToJsonElement(
            """
            {
              "detail": {
                "timestamp": $timestamp,
                "tags": {
                  "account": "acct-1",
                  "region": "eu-west-1",
                  "functionname": "function-a",
                  "pipeline": "pipeline-a"
                },
                "err": {
                  "message": "boom"
                }
              }
            }
            """.trimIndent(),
        )
        val uow = uow(event = event, notifications = notifications)
        val expectedDate = Instant.ofEpochMilli(timestamp).atZone(ZoneId.systemDefault())
        val expectedTimeBucket = "${expectedDate.year}${expectedDate.monthValue - 1}${expectedDate.dayOfMonth}${expectedDate.hour}"
        val expectedDeduplicationId = "function-apipeline-a${expectedTimeBucket}boom"

        // Act
        val result = invokeUowFunction("addNotification", uow)

        // Assert
        result shouldBe uow
        notifications shouldContainKey expectedDeduplicationId

        val notification = notifications.getValue(expectedDeduplicationId)
        notification.stringProperty("subject") shouldBe "Fault: acct-1,eu-west-1,function-a,pipeline-a"
        notification.stringProperty("messageDeduplicationId") shouldBe expectedDeduplicationId
        notification.stringProperty("messageGroupId") shouldBe "Fault: acct-1,eu-west-1,function-a,pipeline-a"
        json.parseToJsonElement(notification.stringProperty("message")) shouldBe event
    }

    @Test
    fun `addNotification uses defaults when tags and error are missing`() {
        // Arrange
        val notifications = linkedMapOf<String, Any>()
        val event = json.parseToJsonElement(
            """
            {
              "detail": {
                "timestamp": 0
              }
            }
            """.trimIndent(),
        )
        val uow = uow(event = event, notifications = notifications)
        val expectedDate = Instant.ofEpochMilli(0).atZone(ZoneId.systemDefault())
        val expectedTimeBucket = "${expectedDate.year}${expectedDate.monthValue - 1}${expectedDate.dayOfMonth}${expectedDate.hour}"
        val expectedDeduplicationId = "undefinedundefined$expectedTimeBucket"

        // Act
        invokeUowFunction("addNotification", uow)

        // Assert
        notifications.keys.shouldContainExactly(expectedDeduplicationId)

        val notification = notifications.getValue(expectedDeduplicationId)
        notification.stringProperty("subject") shouldBe "Fault: undefined,undefined,undefined,undefined"
        notification.stringProperty("messageDeduplicationId") shouldBe expectedDeduplicationId
        notification.stringProperty("messageGroupId") shouldBe "Fault: undefined,undefined,undefined,undefined"
        json.parseToJsonElement(notification.stringProperty("message")) shouldBe event
    }

    @Test
    fun `addNotification ignores events without object detail`() {
        // Arrange
        val notifications = linkedMapOf<String, Any>()
        val cases = listOf(
            json.parseToJsonElement("""["not-an-object"]"""),
            json.parseToJsonElement("""{"detail":"not-an-object"}"""),
            json.parseToJsonElement("""{"message":"no detail"}"""),
        )

        // Act
        cases.forEach { event ->
            invokeUowFunction("addNotification", uow(event = event, notifications = notifications))
        }

        // Assert
        notifications shouldBe emptyMap()
    }

    private fun uow(
        recordId: String = "record-1",
        originalData: String = "",
        data: String? = null,
        eventAsString: String? = null,
        event: JsonElement? = null,
        notifications: MutableMap<String, Any> = linkedMapOf(),
        err: Throwable? = null,
    ): Any {
        val constructor = uowClass.getDeclaredConstructor(
            String::class.java,
            String::class.java,
            String::class.java,
            String::class.java,
            JsonElement::class.java,
            Context::class.java,
            MutableMap::class.java,
            Throwable::class.java,
        )
        constructor.isAccessible = true

        return constructor.newInstance(
            recordId,
            originalData,
            data,
            eventAsString,
            event,
            context,
            notifications,
            err,
        )
    }

    private fun invokeUowFunction(name: String, uow: Any): Any {
        val method = Transform::class.java.privateMethod(name, uowClass)
        return method.invoke(transform, uow)
    }

    private fun invokeJsonFunction(name: String, element: JsonElement): JsonElement {
        val method = Transform::class.java.privateMethod(name, JsonElement::class.java)
        return method.invoke(transform, element) as JsonElement
    }

    private fun invokeStringFunction(name: String, value: String): String {
        val method = Transform::class.java.privateMethod(name, String::class.java)
        return method.invoke(transform, value) as String
    }

    private fun Class<*>.privateMethod(name: String, vararg parameterTypes: Class<*>): Method {
        return getDeclaredMethod(name, *parameterTypes).apply {
            isAccessible = true
        }
    }

    private fun Any.stringProperty(name: String): String {
        return property(name) as String
    }

    private fun Any.jsonElementProperty(name: String): JsonElement {
        return property(name) as JsonElement
    }

    private fun Any.property(name: String): Any? {
        val getterName = "get${name.replaceFirstChar { it.uppercaseChar() }}"
        return javaClass.getDeclaredMethod(getterName).apply {
            isAccessible = true
        }.invoke(this)
    }

    private fun compressed(value: String): String {
        val output = ByteArrayOutputStream()

        GZIPOutputStream(output).use { gzip ->
            gzip.write(value.toByteArray(StandardCharsets.UTF_8))
        }

        return Base64.getEncoder().encodeToString(output.toByteArray())
    }

    private companion object {
        private val uowClass = Class.forName("org.myorg.sut.Transform\$TransformUnitOfWork")
    }
}