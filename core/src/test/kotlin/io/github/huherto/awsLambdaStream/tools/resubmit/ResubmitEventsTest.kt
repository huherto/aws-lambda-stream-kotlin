package io.github.huherto.awsLambdaStream.tools.resubmit

import aws.sdk.kotlin.services.lambda.model.InvocationType
import aws.sdk.kotlin.services.lambda.model.InvokeRequest
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.collections.shouldContainExactly
import io.kotest.matchers.maps.shouldContainExactly
import io.kotest.matchers.nulls.shouldBeNull
import io.kotest.matchers.nulls.shouldNotBeNull
import io.kotest.matchers.shouldBe
import kotlinx.serialization.json.*
import org.junit.jupiter.api.Test

class ResubmitEventsTest {

    private val subject = ResubmitEvents()
    private val json = Json { ignoreUnknownKeys = true }

    @Test
    fun `filterByFunctionName should match wildcard and exact function names`() {
        // Arrange
        val alpha = unitOfWork(functionName = "alpha")
        val beta = unitOfWork(functionName = "beta")

        val wildcardFilter = subject.filterByFunctionName(
            ResubmitEvents.Args(prefix = "prefix", functionname = "*")
        )
        val alphaFilter = subject.filterByFunctionName(
            ResubmitEvents.Args(prefix = "prefix", functionname = "alpha")
        )
        val missingFilter = subject.filterByFunctionName(
            ResubmitEvents.Args(prefix = "prefix", functionname = null)
        )

        // Act
        val wildcardResults = listOf(alpha, beta).map(wildcardFilter)
        val alphaResults = listOf(alpha, beta).map(alphaFilter)
        val missingResult = missingFilter(alpha)

        // Assert
        wildcardResults shouldContainExactly listOf(true, true)
        alphaResults shouldContainExactly listOf(true, false)
        missingResult shouldBe false
    }

    @Test
    fun `hasRecord should detect single record and batch record shapes`() {
        // Arrange
        val singleRecord = unitOfWork(
            event = eventJson(
                uowJson = """
                    {
                      "record": {
                        "eventID": "single"
                      }
                    }
                """.trimIndent()
            )
        )
        val batchRecord = unitOfWork(
            event = eventJson(
                uowJson = """
                    {
                      "batch": [
                        {
                          "record": {
                            "eventID": "batch-1"
                          }
                        },
                        {
                          "record": {
                            "eventID": "batch-2"
                          }
                        }
                      ]
                    }
                """.trimIndent()
            )
        )
        val batchWithoutFirstRecord = unitOfWork(
            event = eventJson(
                uowJson = """
                    {
                      "batch": [
                        {
                          "other": "value"
                        }
                      ]
                    }
                """.trimIndent()
            )
        )
        val emptyBatch = unitOfWork(
            event = eventJson(
                uowJson = """
                    {
                      "batch": []
                    }
                """.trimIndent()
            )
        )
        val missingUow = ResubmitEvents.UnitOfWork(event = jsonObject("""{"type":"fault"}"""))

        // Act
        val results = listOf(
            subject.hasRecord(singleRecord),
            subject.hasRecord(batchRecord),
            subject.hasRecord(batchWithoutFirstRecord),
            subject.hasRecord(emptyBatch),
            subject.hasRecord(missingUow),
        )

        // Assert
        results shouldContainExactly listOf(true, true, false, false, false)
    }

    @Test
    fun `withInvokeRequest should create request for single record`() {
        // Arrange
        val uow = unitOfWork(
            functionName = "target-lambda",
            event = eventJson(
                functionName = "target-lambda",
                uowJson = """
                    {
                      "record": {
                        "eventID": "single",
                        "dynamodb": {
                          "Keys": {
                            "id": {
                              "S": "1"
                            }
                          }
                        }
                      }
                    }
                """.trimIndent()
            )
        )
        val argv = ResubmitEvents.Args(
            prefix = "prefix",
            qualifier = "live",
        )

        // Act
        val result = subject.withInvokeRequest(uow, argv)

        // Assert
        result.recordCount shouldBe 1

        val request = result.invokeRequest.shouldNotBeNull()
        request.functionName shouldBe "target-lambda"
        request.qualifier shouldBe "live"
        request.invocationType shouldBe InvocationType.RequestResponse

        val payload = request.payload.decodePayload()
        payload.jsonObject["Records"].shouldNotBeNull().jsonArray.size shouldBe 1
        payload.jsonObject["Records"].shouldNotBeNull().jsonArray[0]
            .jsonObject["eventID"]
            .shouldNotBeNull()
            .jsonPrimitive
            .content shouldBe "single"
    }

    @Test
    fun `withInvokeRequest should create request for batch records`() {
        // Arrange
        val uow = unitOfWork(
            functionName = "batch-lambda",
            event = eventJson(
                functionName = "batch-lambda",
                uowJson = """
                    {
                      "batch": [
                        {
                          "record": {
                            "eventID": "batch-1"
                          }
                        },
                        {
                          "record": {
                            "eventID": "batch-2"
                          }
                        }
                      ]
                    }
                """.trimIndent()
            )
        )
        val argv = ResubmitEvents.Args(prefix = "prefix")

        // Act
        val result = subject.withInvokeRequest(uow, argv)

        // Assert
        result.recordCount shouldBe 2

        val request = result.invokeRequest.shouldNotBeNull()
        request.functionName shouldBe "batch-lambda"
        request.invocationType shouldBe InvocationType.RequestResponse

        val records = request.payload.decodePayload()
            .jsonObject["Records"]
            .shouldNotBeNull()
            .jsonArray

        records.map { it.jsonObject["eventID"].shouldNotBeNull().jsonPrimitive.content } shouldContainExactly
                listOf("batch-1", "batch-2")
    }

    @Test
    fun `withInvokeRequest should select dry run async and request-response invocation types`() {
        // Arrange
        val uow = unitOfWork(
            functionName = "target-lambda",
            event = eventJson(
                functionName = "target-lambda",
                uowJson = """
                    {
                      "record": {
                        "eventID": "single"
                      }
                    }
                """.trimIndent()
            )
        )
        val dryArgs = ResubmitEvents.Args(prefix = "prefix", dry = true, async = true)
        val asyncArgs = ResubmitEvents.Args(prefix = "prefix", async = true)
        val syncArgs = ResubmitEvents.Args(prefix = "prefix")

        // Act
        val dryResult = subject.withInvokeRequest(uow, dryArgs)
        val asyncResult = subject.withInvokeRequest(uow, asyncArgs)
        val syncResult = subject.withInvokeRequest(uow, syncArgs)

        // Assert
        dryResult.invokeRequest.shouldNotBeNull().invocationType shouldBe InvocationType.DryRun
        asyncResult.invokeRequest.shouldNotBeNull().invocationType shouldBe InvocationType.Event
        syncResult.invokeRequest.shouldNotBeNull().invocationType shouldBe InvocationType.RequestResponse
    }

    @Test
    fun `withInvokeRequest should fall back to request-response when async payload is too large`() {
        // Arrange
        val largeValue = "x".repeat(100_001)
        val uow = unitOfWork(
            functionName = "target-lambda",
            event = eventJson(
                functionName = "target-lambda",
                uowJson = """
                    {
                      "record": {
                        "eventID": "large",
                        "value": "$largeValue"
                      }
                    }
                """.trimIndent()
            )
        )
        val argv = ResubmitEvents.Args(prefix = "prefix", async = true)

        // Act
        val result = subject.withInvokeRequest(uow, argv)

        // Assert
        result.invokeRequest.shouldNotBeNull().invocationType shouldBe InvocationType.RequestResponse
    }

    @Test
    fun `withInvokeRequest should fail when required event fields are missing`() {
        // Arrange
        val missingUow = ResubmitEvents.UnitOfWork(
            event = jsonObject(
                """
                    {
                      "type": "fault",
                      "tags": {
                        "functionname": "target-lambda"
                      }
                    }
                """.trimIndent()
            )
        )
        val missingFunctionName = ResubmitEvents.UnitOfWork(
            event = jsonObject(
                """
                    {
                      "type": "fault",
                      "tags": {},
                      "uow": {
                        "record": {
                          "eventID": "single"
                        }
                      }
                    }
                """.trimIndent()
            )
        )
        val argv = ResubmitEvents.Args(prefix = "prefix")

        // Act
        val missingUowError = shouldThrow<IllegalStateException> {
            subject.withInvokeRequest(missingUow, argv)
        }
        val missingFunctionNameError = shouldThrow<IllegalStateException> {
            subject.withInvokeRequest(missingFunctionName, argv)
        }

        // Assert
        missingUowError.message shouldBe "Missing event.uow"
        missingFunctionNameError.message shouldBe "Missing event.tags.functionname"
    }

    @Test
    fun `errors should attach non-expired-token errors and rethrow expired token errors`() {
        // Arrange
        val uow = unitOfWork(functionName = "target-lambda")
        val regularError = IllegalArgumentException("boom")
        val expiredTokenError = IllegalStateException("The provided token has expired.")

        // Act
        val result = subject.errors(regularError, uow)
        val thrown = shouldThrow<IllegalStateException> {
            subject.errors(expiredTokenError, uow)
        }

        // Assert
        result.err shouldBe regularError
        thrown shouldBe expiredTokenError
    }

    @Test
    fun `count should accumulate event type function invocation record and error counters`() {
        // Arrange
        val counters = ResubmitEvents.Counters()
        val first = unitOfWork(
            functionName = "lambda-a",
            pipeline = "pipeline-a",
            type = "TypeA",
        ).copy(
            recordCount = 2,
            invokeRequest = InvokeRequest {
                functionName = "lambda-a"
            },
            invokeResponseStatusCode = 200,
        )
        val second = unitOfWork(
            functionName = "lambda-a",
            pipeline = "pipeline-a",
            type = "TypeA",
        ).copy(
            recordCount = 1,
            invokeRequest = InvokeRequest {
                functionName = "lambda-a"
            },
            invokeResponseStatusCode = 202,
            err = IllegalStateException("boom"),
        )
        val third = unitOfWork(
            functionName = "lambda-b",
            pipeline = "pipeline-b",
            type = "TypeB",
        ).copy(
            invokeRequest = InvokeRequest {
                functionName = "lambda-b"
            },
            invokeResponseStatusCode = 200,
        )

        // Act
        subject.count(counters, first)
        subject.count(counters, second)
        val result = subject.count(counters, third)

        // Assert
        result shouldBe counters
        counters.match shouldBe 3
        counters.recordCount shouldBe 3
        counters.types shouldContainExactly mapOf(
            "TypeA" to 2,
            "TypeB" to 1,
        )
        counters.functions shouldContainExactly mapOf(
            "lambda-a|pipeline-a" to 2,
            "lambda-b|pipeline-b" to 1,
        )

        val invoked = counters.invoked.shouldNotBeNull()
        invoked.total shouldBe 3
        invoked.statuses shouldContainExactly mapOf(
            200 to 2,
            202 to 1,
        )

        counters.errors shouldBe 1
        counters.errored shouldContainExactly listOf(second)
    }

    @Test
    fun `count should use unknown values when event tags are missing`() {
        // Arrange
        val counters = ResubmitEvents.Counters()
        val uow = ResubmitEvents.UnitOfWork(
            event = jsonObject(
                """
                    {
                      "type": "TypeWithoutTags"
                    }
                """.trimIndent()
            )
        )

        // Act
        subject.count(counters, uow)

        // Assert
        counters.match shouldBe 1
        counters.types shouldContainExactly mapOf("TypeWithoutTags" to 1)
        counters.functions shouldContainExactly mapOf("unknown|unknown" to 1)
        counters.invoked.shouldBeNull()
        counters.errors shouldBe 0
    }

    @Test
    fun `splitLines should split non-blank lines and keep uow unchanged when text is missing`() {
        // Arrange
        val multiline = ResubmitEvents.UnitOfWork(
            getResponseLine = """
                first
                
                second
                  
                third
            """.trimIndent()
        )
        val missingText = ResubmitEvents.UnitOfWork()

        // Act
        val split = subject.splitLines(multiline)
        val unchanged = subject.splitLines(missingText)

        // Assert
        split.map { it.getResponseLine } shouldContainExactly listOf("first", "second", "third")
        unchanged shouldContainExactly listOf(missingText)
    }

    @Test
    fun `json object value helpers should return primitive values only`() {
        // Arrange
        val obj = jsonObject(
            """
                {
                  "string": "value",
                  "boolean": true,
                  "int": 42,
                  "nested": {
                    "value": "ignored"
                  },
                  "array": [1, 2]
                }
            """.trimIndent()
        )

        // Act
        val stringValue = obj.stringValue("string")
        val booleanValue = obj.booleanValue("boolean")
        val intValue = obj.intValue("int")
        val nestedAsString = obj.stringValue("nested")
        val arrayAsInt = obj.intValue("array")
        val missingBoolean = obj.booleanValue("missing")

        // Assert
        stringValue shouldBe "value"
        booleanValue shouldBe true
        intValue shouldBe 42
        nestedAsString.shouldBeNull()
        arrayAsInt.shouldBeNull()
        missingBoolean.shouldBeNull()
    }

    private fun unitOfWork(
        functionName: String = "target-lambda",
        pipeline: String = "target-pipeline",
        type: String = "fault",
        event: JsonObject = eventJson(
            functionName = functionName,
            pipeline = pipeline,
            type = type,
        ),
    ): ResubmitEvents.UnitOfWork {
        return ResubmitEvents.UnitOfWork(event = event)
    }

    private fun eventJson(
        functionName: String = "target-lambda",
        pipeline: String = "target-pipeline",
        type: String = "fault",
        uowJson: String = """
            {
              "record": {
                "eventID": "single"
              }
            }
        """.trimIndent(),
    ): JsonObject {
        return jsonObject(
            """
                {
                  "type": "$type",
                  "tags": {
                    "functionname": "$functionName",
                    "pipeline": "$pipeline"
                  },
                  "uow": $uowJson
                }
            """.trimIndent()
        )
    }

    private fun jsonObject(value: String): JsonObject {
        return json.parseToJsonElement(value).jsonObject
    }

    private fun ByteArray?.decodePayload(): JsonObject {
        return json.parseToJsonElement(
            this.shouldNotBeNull().decodeToString()
        ).jsonObject
    }
}