package io.github.huherto.awsLambdaStream

import aws.sdk.kotlin.services.lambda.model.InvocationType
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.maps.shouldContainExactly
import io.kotest.matchers.nulls.shouldNotBeNull
import io.kotest.matchers.shouldBe
import kotlinx.serialization.json.*
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test

class MainTest {
    private val json = Json {
        ignoreUnknownKeys = true
    }

    @Suppress("UNCHECKED_CAST")
    private fun <T> callPrivate(name: String, vararg args: Any?): T {
        val method = Class.forName("io.gthub.huherto.awsLambdaStream.MainKt")
            .declaredMethods
            .first { method ->
                method.name == name && method.parameterCount == args.size
            }

        method.isAccessible = true

        return method.invoke(null, *args) as T
    }

    private fun event(
        functionName: String = "target-function",
        pipeline: String = "pipeline-a",
        type: String = "Faulted",
        uow: JsonObject = JsonObject(
            mapOf(
                "record" to JsonObject(
                    mapOf("eventID" to JsonPrimitive("record-1"))
                )
            )
        ),
    ): JsonObject =
        JsonObject(
            mapOf(
                "type" to JsonPrimitive(type),
                "tags" to JsonObject(
                    mapOf(
                        "functionname" to JsonPrimitive(functionName),
                        "pipeline" to JsonPrimitive(pipeline),
                    )
                ),
                "uow" to uow,
            )
        )

    private fun singleRecordUow(
        functionName: String = "target-function",
        recordId: String = "record-1",
    ): UnitOfWork =
        UnitOfWork(
            event = event(
                functionName = functionName,
                uow = JsonObject(
                    mapOf(
                        "record" to JsonObject(
                            mapOf("eventID" to JsonPrimitive(recordId))
                        )
                    )
                )
            )
        )

    private fun batchRecordUow(
        functionName: String = "target-function",
        recordIds: List<String> = listOf("record-1", "record-2"),
    ): UnitOfWork =
        UnitOfWork(
            event = event(
                functionName = functionName,
                uow = JsonObject(
                    mapOf(
                        "batch" to JsonArray(
                            recordIds.map { recordId ->
                                JsonObject(
                                    mapOf(
                                        "record" to JsonObject(
                                            mapOf("eventID" to JsonPrimitive(recordId))
                                        )
                                    )
                                )
                            }
                        )
                    )
                )
            )
        )

    @Nested
    inner class FilterByFunctionName {

        @Test
        fun `should accept wildcard and matching function name and reject different function name`() {
            // Arrange
            val wildcardArgs = Args(prefix = "prefix", functionname = "*")
            val matchingArgs = Args(prefix = "prefix", functionname = "target-function")
            val differentArgs = Args(prefix = "prefix", functionname = "other-function")
            val uow = singleRecordUow(functionName = "target-function")

            // Act
            val wildcardFilter = filterByFunctionName(wildcardArgs)
            val matchingFilter = filterByFunctionName(matchingArgs)
            val differentFilter = filterByFunctionName(differentArgs)

            // Assert
            wildcardFilter(uow) shouldBe true
            matchingFilter(uow) shouldBe true
            differentFilter(uow) shouldBe false
        }

        @Test
        fun `should reject events without function name tag unless wildcard is used`() {
            // Arrange
            val wildcardArgs = Args(prefix = "prefix", functionname = "*")
            val matchingArgs = Args(prefix = "prefix", functionname = "target-function")
            val uow = UnitOfWork(
                event = JsonObject(
                    mapOf(
                        "tags" to JsonObject(emptyMap()),
                        "uow" to JsonObject(
                            mapOf(
                                "record" to JsonObject(emptyMap())
                            )
                        )
                    )
                )
            )

            // Act
            val wildcardFilter = filterByFunctionName(wildcardArgs)
            val matchingFilter = filterByFunctionName(matchingArgs)

            // Assert
            wildcardFilter(uow) shouldBe true
            matchingFilter(uow) shouldBe false
        }
    }

    @Nested
    inner class HasRecord {

        @Test
        fun `should detect single record and batch record`() {
            // Arrange
            val singleRecord = singleRecordUow()
            val batchRecord = batchRecordUow()

            // Act
            val singleResult = hasRecord(singleRecord)
            val batchResult = hasRecord(batchRecord)

            // Assert
            singleResult shouldBe true
            batchResult shouldBe true
        }

        @Test
        fun `should reject missing uow empty batch and batch without record`() {
            // Arrange
            val missingUow = UnitOfWork(event = JsonObject(emptyMap()))
            val emptyBatch = UnitOfWork(
                event = event(
                    uow = JsonObject(
                        mapOf("batch" to JsonArray(emptyList()))
                    )
                )
            )
            val batchWithoutRecord = UnitOfWork(
                event = event(
                    uow = JsonObject(
                        mapOf(
                            "batch" to JsonArray(
                                listOf(
                                    JsonObject(
                                        mapOf("notRecord" to JsonPrimitive("value"))
                                    )
                                )
                            )
                        )
                    )
                )
            )

            // Act
            val missingUowResult = hasRecord(missingUow)
            val emptyBatchResult = hasRecord(emptyBatch)
            val batchWithoutRecordResult = hasRecord(batchWithoutRecord)

            // Assert
            missingUowResult shouldBe false
            emptyBatchResult shouldBe false
            batchWithoutRecordResult shouldBe false
        }
    }

    @Nested
    inner class WithInvokeRequest {

        @Test
        fun `should create request response invoke request for a single record`() {
            // Arrange
            val argv = Args(
                prefix = "prefix",
                qualifier = "live",
                dry = false,
                async = false,
            )
            val uow = singleRecordUow(
                functionName = "target-function",
                recordId = "record-1",
            )

            // Act
            val result = withInvokeRequest(uow, argv)

            // Assert
            val request = result.invokeRequest.shouldNotBeNull()
            result.recordCount shouldBe 1
            request.functionName shouldBe "target-function"
            request.qualifier shouldBe "live"
            request.invocationType shouldBe InvocationType.RequestResponse

            val payload = json.parseToJsonElement(request.payload.shouldNotBeNull().decodeToString()).jsonObject
            payload["Records"].shouldNotBeNull().jsonArray shouldHaveSize 1
            payload["Records"].shouldNotBeNull().jsonArray[0].jsonObject["eventID"] shouldBe JsonPrimitive("record-1")
        }

        @Test
        fun `should create event invoke request for small async batch`() {
            // Arrange
            val argv = Args(
                prefix = "prefix",
                async = true,
            )
            val uow = batchRecordUow(
                functionName = "target-function",
                recordIds = listOf("record-1", "record-2"),
            )

            // Act
            val result = withInvokeRequest(uow, argv)

            // Assert
            val request = result.invokeRequest.shouldNotBeNull()
            result.recordCount shouldBe 2
            request.functionName shouldBe "target-function"
            request.invocationType shouldBe InvocationType.Event

            val payload = json.parseToJsonElement(request.payload.shouldNotBeNull().decodeToString()).jsonObject
            val records = payload["Records"].shouldNotBeNull().jsonArray
            records shouldHaveSize 2
            records[0].jsonObject["eventID"] shouldBe JsonPrimitive("record-1")
            records[1].jsonObject["eventID"] shouldBe JsonPrimitive("record-2")
        }

        @Test
        fun `should prefer dry run invocation type over async`() {
            // Arrange
            val argv = Args(
                prefix = "prefix",
                dry = true,
                async = true,
            )
            val uow = singleRecordUow()

            // Act
            val result = withInvokeRequest(uow, argv)

            // Assert
            result.invokeRequest.shouldNotBeNull().invocationType shouldBe InvocationType.DryRun
        }

        @Test
        fun `should throw when required event fields are missing`() {
            // Arrange
            val argv = Args(prefix = "prefix")
            val missingUow = UnitOfWork(event = JsonObject(emptyMap()))
            val missingFunctionName = UnitOfWork(
                event = JsonObject(
                    mapOf(
                        "uow" to JsonObject(
                            mapOf("record" to JsonObject(emptyMap()))
                        ),
                        "tags" to JsonObject(emptyMap()),
                    )
                )
            )

            // Act & Assert
            shouldThrow<Throwable> {
                withInvokeRequest(missingUow, argv)
            }

            shouldThrow<Throwable> {
                withInvokeRequest(missingFunctionName, argv)
            }
        }
    }

    @Nested
    inner class Count {

        @Test
        fun `should count matched events record counts invocations statuses and errors`() {
            // Arrange
            val counters = Counters()
            val error = RuntimeException("boom")
            val uow = singleRecordUow(functionName = "target-function")
                .copy(
                    recordCount = 3,
                    invokeRequest = aws.sdk.kotlin.services.lambda.model.InvokeRequest {
                        functionName = "target-function"
                        invocationType = InvocationType.RequestResponse
                    },
                    invokeResponseStatusCode = 202,
                    err = error,
                )

            // Act
            val result = count(counters, uow)

            // Assert
            result shouldBe counters
            counters.match shouldBe 1
            counters.recordCount shouldBe 3
            counters.types.shouldContainExactly(mapOf("Faulted" to 1))
            counters.functions.shouldContainExactly(mapOf("target-function|pipeline-a" to 1))

            val invoked = counters.invoked.shouldNotBeNull()
            invoked.total shouldBe 1
            invoked.statuses.shouldContainExactly(mapOf(202 to 1))

            counters.errors shouldBe 1
            counters.errored shouldBe listOf(uow)
        }

        @Test
        fun `should count unknown event metadata and skip invocation status when missing`() {
            // Arrange
            val counters = Counters()
            val uow = UnitOfWork(
                event = JsonObject(
                    mapOf(
                        "type" to JsonPrimitive("Faulted"),
                        "tags" to JsonObject(emptyMap()),
                    )
                ),
                invokeRequest = aws.sdk.kotlin.services.lambda.model.InvokeRequest {
                    functionName = "target-function"
                },
            )

            // Act
            count(counters, uow)

            // Assert
            counters.match shouldBe 1
            counters.types.shouldContainExactly(mapOf("Faulted" to 1))
            counters.functions.shouldContainExactly(mapOf("unknown|unknown" to 1))

            val invoked = counters.invoked.shouldNotBeNull()
            invoked.total shouldBe 1
            invoked.statuses.shouldContainExactly(emptyMap())
            counters.errors shouldBe 0
        }
    }

    @Nested
    inner class SplitLines {

        @Test
        fun `should split non blank response lines into separate units of work`() {
            // Arrange
            val uow = UnitOfWork(getResponseLine = "first\n\nsecond\n   \nthird")

            // Act
            val result = splitLines(uow)

            // Assert
            result shouldHaveSize 3
            result.map { it.getResponseLine } shouldBe listOf("first", "second", "third")
        }

        @Test
        fun `should return original unit of work when response line is missing`() {
            // Arrange
            val uow = UnitOfWork(getResponseLine = null)

            // Act
            val result = splitLines(uow)

            // Assert
            result shouldBe listOf(uow)
        }
    }

    @Nested
    inner class Errors {

        @Test
        fun `should attach non expired token errors to unit of work`() {
            // Arrange
            val error = RuntimeException("boom")
            val uow = singleRecordUow()

            // Act
            val result = errors(error, uow)

            // Assert
            result.err shouldBe error
            result.event shouldBe uow.event
        }

        @Test
        fun `should rethrow expired token errors`() {
            // Arrange
            val error = RuntimeException("The provided token has expired.")
            val uow = singleRecordUow()

            // Act & Assert
            shouldThrow<RuntimeException> {
                errors(error, uow)
            } shouldBe error
        }
    }

    private val JsonElement.jsonObject: JsonObject
        get() = this as JsonObject

    private val JsonElement.jsonArray: JsonArray
        get() = this as JsonArray
}