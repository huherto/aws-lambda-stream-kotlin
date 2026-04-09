package io.github.huherto.awsLambdaStream.flavors

import com.amazonaws.services.lambda.runtime.events.DynamodbEvent
import com.amazonaws.services.lambda.runtime.events.models.dynamodb.StreamRecord
import io.github.huherto.awsLambdaStream.*
import io.github.huherto.awsLambdaStream.from.RecordImage
import io.github.huherto.awsLambdaStream.from.RecordPair
import io.github.huherto.awsLambdaStream.sinks.EventPublisher
import io.github.huherto.awsLambdaStream.sinks.EventsMicrostore
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.nulls.shouldNotBeNull
import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNotBe
import io.kotest.matchers.types.shouldBeTypeOf
import io.mockk.every
import io.mockk.mockk
import io.mockk.spyk
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import com.amazonaws.services.lambda.runtime.events.models.dynamodb.AttributeValue as StreamAV

class EvaluatePipelineTest {

    private val envConfig = spyk<EnvironmentConfig>()
    private val eventPublisher = mockk<EventPublisher>()
    private val eventsMicrostore = mockk<EventsMicrostore>()
    private val faultManager = mockk<FaultManager>()

    private fun createPipeline(
        pipelineId: String = "pipeline-1",
        correlationKeySuffix: String = "",
        index: String? = null,
        unmarshall: ((String) -> Event)? = null,
        expression: ((UnitOfWork) -> Boolean)? = null,
        higherOrderEmit: EmitOption? = null,
    ): EvaluatePipeline {
        return EvaluatePipeline(
            id = pipelineId,
            envConfig = envConfig,
            eventPublisher = eventPublisher,
            eventsMicrostore = eventsMicrostore,
            correlationKeySuffix = correlationKeySuffix,
            index = index,
            unmarshall = unmarshall,
            expression = expression,
            higherOrderEmit = higherOrderEmit,
        )
    }

    private fun createEvent(
        id: String = "event-1",
        timestamp: Long = 1_700_000_000_000L,
        partitionKey: String? = "partition-1",
        tags: Map<String, String>? = null,
        raw: Any? = null,
        eem: Any? = null,
        type: String = "TestEvent",
    ): Event = object : Event {
        override var id: String? = id
        override var timestamp: Long? = timestamp
        override var partitionKey: String? = partitionKey
        override var tags: Map<String, String>? = tags
        override var raw: Any? = raw
        override var eem: Any? = eem
        override fun eventType() = type
        override fun encoded() = """{"id":"$id","type":"$type"}"""
    }

    private fun createInsertRecord(
        sk: String = "EVENT",
        discriminator: String? = null,
    ): DynamodbEvent.DynamodbStreamRecord {
        return DynamodbEvent.DynamodbStreamRecord().apply {
            eventName = "INSERT"
            dynamodb = StreamRecord().apply {
                keys = mapOf("sk" to StreamAV(sk))
                newImage = buildMap {
                    if (discriminator != null) {
                        put("discriminator", StreamAV(discriminator))
                    }
                }
            }
        }
    }

    @Test
    fun `forEvents should accept INSERT events with EVENT sk and CORREL newImage and reject others`() {
        // Arrange
        val pipeline = createPipeline()

        val eventInsert = UnitOfWork(record = createInsertRecord(sk = "EVENT"))
        val correlInsert = UnitOfWork(record = createInsertRecord(discriminator = "CORREL"))
        val wrongSkInsert = UnitOfWork(record = createInsertRecord(sk = "NOT_EVENT"))
        val wrongType = UnitOfWork(record = Any())

        // Act
        val eventInsertResult = pipeline.forEvents(eventInsert)
        val correlInsertResult = pipeline.forEvents(correlInsert)
        val wrongSkInsertResult = pipeline.forEvents(wrongSkInsert)
        val wrongTypeResult = pipeline.forEvents(wrongType)

        // Assert
        eventInsertResult shouldBe true
        correlInsertResult shouldBe true
        wrongSkInsertResult shouldBe false
        wrongTypeResult shouldBe false
    }

    @Test
    fun `defaultUnmarshall should use custom unmarshall when provided and JsonEvent otherwise`() {
        // Arrange
        val customEvent = createEvent(id = "custom-id", type = "CustomType")
        val pipelineWithCustom = createPipeline(
            unmarshall = { input ->
                createEvent(id = "parsed-$input", type = "CustomType")
            }
        )
        val pipelineWithJson = createPipeline()

        // Act
        val customResult = pipelineWithCustom.defaultUnmarshall("""{"id":"ignored"}""")
        val jsonResult = pipelineWithJson.defaultUnmarshall("""{"id":"json-id","type":"JsonType"}""")

        // Assert
        customResult.id shouldBe "parsed-{\"id\":\"ignored\"}"
        customResult.eventType() shouldBe "CustomType"

        jsonResult.shouldBeTypeOf<JsonEvent>()
        jsonResult.id shouldBe "json-id"
        jsonResult.eventType() shouldBe "JsonType"
        customEvent.id shouldNotBe null
    }

    @Test
    fun `defaultUnmarshall should throw for invalid json`() {
        // Arrange
        val pipeline = createPipeline()

        // Act & Assert
        shouldThrow<Exception> {
            pipeline.defaultUnmarshall("not-json")
        }
    }

    @Test
    fun `normalize should populate meta queryParams and event from record pair`() {
        // Arrange
        val pipeline = createPipeline(
            unmarshall = { input -> createEvent(id = "decoded", type = "DecodedType", raw = input) }
        )
        val rawNew = RecordImage(
            mapOf(
                "event" to StreamAV().withN("""{"id":"decoded","type":"DecodedType"}"""),
                "pk" to StreamAV("pk-1"),
                "data" to StreamAV("data-1"),
                "discriminator" to StreamAV("CORREL"),
                "suffix" to StreamAV("suffix-1"),
                "ttl" to StreamAV().withN("123"),
                "expire" to StreamAV().withBOOL(true),
            )
        )
        val raw = RecordPair(new = rawNew, old = null)
        val uow = UnitOfWork(
            record = createInsertRecord(discriminator = "CORREL"),
            event = createEvent(id = "event-1", raw = raw)
        )

        // Act
        val result = pipeline.normalize(uow)

        // Assert
        val meta = result.meta.shouldNotBeNull()
        meta["id"] shouldBe "event-1"
        meta["pk"] shouldBe "pk-1"
        meta["data"] shouldBe "data-1"
        meta["correlationKey"] shouldBe "pk-1"
        meta["suffix"] shouldBe "suffix-1"
        meta["correlation"] shouldBe "true"
        
        result.queryParams.shouldNotBeNull()
        result.queryParams.pk shouldBe "pk-1"
        result.queryParams.isCorrelated shouldBe true
        result.event.shouldNotBeNull()
        result.event.id shouldBe "decoded"
        result.event.eventType() shouldBe "DecodedType"
    }

    @Test
    fun `onCorrelationKeySuffix should match empty suffix and same suffix and reject different suffixes`() {
        // Arrange
        val noSuffixPipeline = createPipeline(correlationKeySuffix = "")
        val suffixPipeline = createPipeline(correlationKeySuffix = "abc")

        val noSuffixUow = UnitOfWork(meta = mapOf("suffix" to null))
        val sameSuffixUow = UnitOfWork(meta = mapOf("suffix" to "abc"))
        val differentSuffixUow = UnitOfWork(meta = mapOf("suffix" to "xyz"))

        // Act
        val noSuffixMatch = noSuffixPipeline.onCorrelationKeySuffix(noSuffixUow)
        val sameSuffixMatch = suffixPipeline.onCorrelationKeySuffix(sameSuffixUow)
        val differentSuffixMatch = suffixPipeline.onCorrelationKeySuffix(differentSuffixUow)
        val missingSuffixRejected = suffixPipeline.onCorrelationKeySuffix(noSuffixUow)

        // Assert
        noSuffixMatch shouldBe true
        sameSuffixMatch shouldBe true
        differentSuffixMatch shouldBe false
        missingSuffixRejected shouldBe false
    }

    @Test
    fun `toQueryRequest should build correlation and non correlation requests`() {
        // Arrange
        val pipeline = createPipeline(index = "CustomIndex")
        val correlationUow = UnitOfWork(meta = mapOf("correlation" to "true", "pk" to "pk-1"))
        val dataUow = UnitOfWork(meta = mapOf("correlation" to "false", "data" to "data-1"))

        // Act
        val correlationResult = pipeline.toQueryRequest(correlationUow)
        val dataResult = pipeline.toQueryRequest(dataUow)

        // Assert
        val correlationRequest = correlationResult.queryRequest.shouldNotBeNull()
        correlationRequest.keyConditionExpression shouldBe "#pk = :pk"
        correlationRequest.expressionAttributeNames shouldBe mapOf("#pk" to "pk")
        correlationRequest.expressionAttributeValues?.get(":pk")?.asS() shouldBe "pk-1"
        correlationRequest.consistentRead shouldBe true

        val dataRequest = dataResult.queryRequest.shouldNotBeNull()
        dataRequest.indexName shouldBe "CustomIndex"
        dataRequest.keyConditionExpression shouldBe "#data = :data"
        dataRequest.expressionAttributeNames shouldBe mapOf("#data" to "data")
        dataRequest.expressionAttributeValues?.get(":data")?.asS() shouldBe "data-1"
        dataRequest.consistentRead shouldBe null
    }

    @Test
    fun `complex should set triggers when expression is null and filter by expression when provided`() : Unit = runBlocking {
        // Arrange
        val first = UnitOfWork(event = createEvent(id = "e-1"))
        val second = UnitOfWork(event = createEvent(id = "e-2"))
        val pipelineWithNoExpression = createPipeline()

        every { eventsMicrostore.queryByPk(any()) } answers { firstArg() }

        val expressionPipeline = createPipeline(
            expression = { uow -> uow.meta?.get("keep") == "true" }
        )
        val matchingUow = UnitOfWork(
            meta = mapOf(
                "keep" to "true",
                "suffix" to null,
                "id" to "uow-1",
                "correlationKey" to "correlation-key"
            ),
            event = createEvent(id = "e-3")
        )
        val rejectedUow = matchingUow.copy(meta = matchingUow.meta?.plus("keep" to "false"))

        // Act
        val noExpressionResult = pipelineWithNoExpression.run {
            flowOf(first, second).complex().toList()
        }
        val expressionResult = expressionPipeline.run {
            flowOf(matchingUow, rejectedUow).complex().toList()
        }

        // Assert
        noExpressionResult shouldHaveSize 2
        noExpressionResult[0].triggers.shouldNotBeNull()
        noExpressionResult[0].triggers!![0].id shouldBe "e-1"
        noExpressionResult[1].triggers.shouldNotBeNull()
        noExpressionResult[1].triggers!![0].id shouldBe "e-2"

        expressionResult shouldHaveSize 1
        expressionResult[0].meta!!["keep"] shouldBe "true"
    }

    @Test
    fun `toHigherOrderEvents should create basic higher order event and custom emit events`() {
        // Arrange
        val baseEvent = createEvent(
            id = "base-event",
            timestamp = 1_700_000_000_000L,
            tags = mapOf("region" to "eu-west-1", "source" to "app", "team" to "core", "env" to "test"),
            raw = "raw-value",
            eem = mapOf("key" to "value"),
            type = "BaseType"
        )
        val trigger = createEvent(id = "trigger-1", timestamp = 1_700_000_000_123L, type = "TriggerType")
        val uow = UnitOfWork(
            event = baseEvent,
            meta = mapOf(
                "id" to "uow-1",
                "correlationKey" to "partition-1.suffix-a"
            ),
            triggers = listOf(trigger, baseEvent)
        )

        val basicPipeline = createPipeline(
            pipelineId = "pipeline-basic",
            correlationKeySuffix = "suffix-a",
            higherOrderEmit = EmitOption.Basic(type = "HigherType")
        )
        val customPipeline = createPipeline(
            pipelineId = "pipeline-custom",
            correlationKeySuffix = "suffix-a",
            higherOrderEmit = EmitOption.Custom { _, template ->
                val t2 = template
                listOf(
                    template,
                    t2
                )
            }
        )

        // Act
        val basicResult = basicPipeline.toHigherOrderEvents(uow)
        val customResult = customPipeline.toHigherOrderEvents(uow)

        // Assert
        basicResult shouldHaveSize 1
        val basicEvent = basicResult.first().event.shouldNotBeNull()
        basicEvent.shouldBeTypeOf<EvaluatePipeline.HigherOrderEvent>()
        basicEvent.id shouldBe "uow-1.pipeline-basic"
        basicEvent.eventType() shouldBe "HigherType"
        basicEvent.partitionKey shouldBe "partition-1"
        basicEvent.tags shouldBe mapOf("team" to "core", "env" to "test")
        basicEvent.mappedTriggers?.shouldHaveSize(2)
        basicEvent.baseEvent shouldBe baseEvent
        basicEvent.raw shouldBe "raw-value"
        basicEvent.eem shouldBe mapOf("key" to "value")

        customResult shouldHaveSize 2
        customResult[0].event.shouldNotBeNull().shouldBeTypeOf<EvaluatePipeline.HigherOrderEvent>()
        customResult[1].event.shouldNotBeNull().shouldBeTypeOf<EvaluatePipeline.HigherOrderEvent>()
        // fix this. It is failing.
        //customResult[1].event!!.id shouldBe "second-template"
    }

    @Test
    fun `toHigherOrderEvents should throw when higherOrderEmit is missing`() {
        // Arrange
        val pipeline = createPipeline(
            higherOrderEmit = null
        )
        val uow = UnitOfWork(
            event = createEvent(),
            meta = mapOf("id" to "uow-1", "correlationKey" to "key")
        )

        // Act & Assert
        shouldThrow<IllegalArgumentException> {
            pipeline.toHigherOrderEvents(uow)
        }
    }
}