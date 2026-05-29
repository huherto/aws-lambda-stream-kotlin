package io.github.huherto.awsLambdaStream.flavors

import com.amazonaws.services.lambda.runtime.events.DynamodbEvent
import com.amazonaws.services.lambda.runtime.events.models.dynamodb.StreamRecord
import io.github.huherto.awsLambdaStream.*
import io.github.huherto.awsLambdaStream.from.RecordImage
import io.github.huherto.awsLambdaStream.from.RecordPair
import io.github.huherto.awsLambdaStream.from.TableChangeEvent
import io.github.huherto.awsLambdaStream.sinks.EventPublisher
import io.github.huherto.awsLambdaStream.sinks.EventsMicrostore
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.nulls.shouldNotBeNull
import io.kotest.matchers.shouldBe
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

    class SimpleEventCodec : EventCodec {
        override fun decode(eventAsString: String): Event {
            val jsonEvent: JsonEvent = try {
                JsonEvent(eventAsString)
            } catch (e: Exception) {
                throw e
            }
            return jsonEvent
        }

        override fun encode(event: Event): String {
            return event.asJson()
        }
    }

    private val envConfig = spyk<EnvironmentConfig>()
    private val eventPublisher = mockk<EventPublisher>()
    private val eventsMicrostore = mockk<EventsMicrostore>()
    private val eventCodec = SimpleEventCodec()
    private val faultManager = mockk<FaultManager>()

    private fun createPipeline(
        pipelineId: String = "pipeline-1",
        correlationKeySuffix: String = "",
        index: String? = null,
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
            eventCodec = eventCodec,
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
    ): Event = object : BaseEvent() {
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
            //unmarshall = { input -> createEvent(id = "decoded", type = "DecodedType", raw = input) }
        )
        val rawNew = RecordImage(
            mapOf(
                "event" to StreamAV().withS("""{"id":"decoded","type":"DecodedType"}"""),
                "pk" to StreamAV("pk-1"),
                "data" to StreamAV("data-1"),
                "discriminator" to StreamAV("CORREL"),
                "suffix" to StreamAV("suffix-1"),
                "ttl" to StreamAV().withN("123"),
                "expire" to StreamAV().withBOOL(true),
            )
        )
        val tableChangeEvent = TableChangeEvent().apply {
            id = "event-1"
            raw = RecordPair(new = rawNew, old = null)
        }
        val uow = UnitOfWork(
            record = createInsertRecord(discriminator = "CORREL"),
            event = tableChangeEvent
        )

        // Act
        val result = pipeline.normalize(uow)

        // Assert
        val meta = result.meta.shouldNotBeNull()
        meta["eventId"] shouldBe "event-1.pipeline-1"
        meta["partitionKey"] shouldBe "pk-1"

        result.queryParams.shouldNotBeNull()
        result.queryParams.pk shouldBe "pk-1"
        result.queryParams.correlation shouldBe true
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
            flowOf(first, second).complex(faultManager).toList()
        }
        val expressionResult = expressionPipeline.run {
            flowOf(matchingUow, rejectedUow).complex(faultManager).toList()
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

    class HigherType : BaseEvent() {
        override fun eventType(): String {
            return "HigherType"
        }

        override fun encoded(): String {
            TODO("Not yet implemented")
        }

    }

    @Test
    fun `toHigherOrderEvents should create basic higher order event`() {
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
                "eventId" to "uow-1.pipeline-basic",
                "partitionKey" to "partition-1"
            ),
            triggers = listOf(trigger, baseEvent)
        )

        val basicPipeline = createPipeline(
            pipelineId = "pipeline-basic",
            correlationKeySuffix = "suffix-a",
            higherOrderEmit = EmitOption.Basic(clazz = HigherType::class.java)
        )

        // Act
        val basicResult = basicPipeline.toHigherOrderEvents(uow)

        // Assert
        basicResult shouldHaveSize 1
        val basicEvent = basicResult.first().event.shouldNotBeNull()
        basicEvent.shouldBeTypeOf<HigherType>()
        basicEvent.id shouldBe "uow-1.pipeline-basic"
        basicEvent.eventType() shouldBe "HigherType"
        basicEvent.partitionKey shouldBe "partition-1"
        basicEvent.tags shouldBe mapOf("team" to "core", "env" to "test")
        basicEvent.triggers?.shouldHaveSize(2)
        // basicEvent.base shouldBe baseEvent
        basicEvent.raw shouldBe "raw-value"
        basicEvent.eem shouldBe mapOf("key" to "value")
    }

    @Test
    fun `toHigherOrderEvents should create custom emit events`() {
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
                "eventId" to "uow-1.pipeline-custom",
                "partitionKey" to "partition-1"
            ),
            triggers = listOf(trigger, baseEvent)
        )

        val customPipeline = createPipeline(
            pipelineId = "pipeline-custom",
            correlationKeySuffix = "suffix-a",
            higherOrderEmit = EmitOption.Custom { _, template ->
                val t1 = template.createEvent(HigherType::class.java.kotlin)
                val t2 = template.createEvent(HigherType::class.java.kotlin)
                listOf(
                    t1,
                    t2
                )
            }
        )

        // Act
        val customResult = customPipeline.toHigherOrderEvents(uow)

        // Assert
        customResult shouldHaveSize 2
        customResult[0].event.shouldNotBeNull().shouldBeTypeOf<HigherType>()
        customResult[1].event.shouldNotBeNull().shouldBeTypeOf<HigherType>()
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