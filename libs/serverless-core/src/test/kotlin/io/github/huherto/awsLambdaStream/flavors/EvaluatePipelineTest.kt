package io.github.huherto.awsLambdaStream.flavors

import com.amazonaws.services.lambda.runtime.events.DynamodbEvent
import com.amazonaws.services.lambda.runtime.events.models.dynamodb.StreamRecord
import io.github.huherto.awsLambdaStream.*
import io.github.huherto.awsLambdaStream.connectors.EventBridgeConnector
import io.github.huherto.awsLambdaStream.from.RecordImage
import io.github.huherto.awsLambdaStream.from.RecordPair
import io.github.huherto.awsLambdaStream.sinks.EventPublisherInMemory
import io.github.huherto.awsLambdaStream.sinks.EventsMicrostoreInMemory
import io.kotest.matchers.nulls.shouldNotBeNull
import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.shouldBeInstanceOf
import io.mockk.*
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import aws.sdk.kotlin.services.dynamodb.model.AttributeValue as SdkAV
import com.amazonaws.services.lambda.runtime.events.models.dynamodb.AttributeValue as EventAV


class EvaluatePipelineTest {

    @BeforeEach
    fun setupMocking() {
        // Prevent AWS client actual network instantiation
        mockkObject(EventBridgeConnector.Companion)
        every { EventBridgeConnector.getClient(any(), any()) } returns mockk(relaxed = true)
    }

    @AfterEach
    fun tearDown() {
        unmockkAll()
    }

    private  val envConfig = spyk<EnvironmentConfig>()

    fun protoEvaluatePipeline(id: String) : EvaluatePipeline {
        val eventPublisher = EventPublisherInMemory()
        val pipeline = EvaluatePipeline(
            id = id,
            envConfig = envConfig,
            eventPublisher = eventPublisher,
            eventsMicrostore = EventsMicrostoreInMemory(),
            )
        return pipeline
    }

    @Test
    fun `normalize should extract metadata correctly for non-CORREL events`() {
        // Arrange
        val pipeline = protoEvaluatePipeline("test-di")
        val eventAsString = """{"id": "ev1", "type": "TestEvent"}"""
        val rawNewMap = mapOf(
            "event" to EventAV().withS(eventAsString),
            "discriminator" to EventAV().withS("EVENT"),
            "pk" to EventAV().withS("pk-123"),
            "data" to EventAV().withS("data-456"),
            "ttl" to EventAV().withN("1234567890"),
            "expire" to EventAV().withB(java.nio.ByteBuffer.wrap("exp".toByteArray())),
            "suffix" to EventAV().withS("sfx")
        )
        val uow = UnitOfWork(
            event = object : Event {
                override var id: String? = "ev1"
                override var timestamp: Long? = null
                override var partitionKey: String? = null
                override var tags: Map<String, String>? = null
                override var raw: Any? = RecordPair(new = RecordImage(rawNewMap), old = null)
                override var eem: Any? = null
                override fun eventType() = "TestEvent"
                override fun encoded() = ""
            },
            record = DynamodbEvent.DynamodbStreamRecord().apply {
                dynamodb = StreamRecord().apply { sequenceNumber = "seq-999" }
            }
        )

        // Act
        val result = pipeline.normalize(uow)

        // Assert
        result.event.shouldBeInstanceOf<JsonEvent>()
        result.event.id shouldBe "ev1"
        result.meta?.get("id") shouldBe "ev1"
        result.meta?.get("sequenceNumber") shouldBe "seq-999"
        result.meta?.get("ttl") shouldBe "1234567890"
        result.meta?.get("expire") shouldBe java.nio.ByteBuffer.wrap("exp".toByteArray()).toString()
        result.meta?.get("pk") shouldBe "pk-123"
        result.meta?.get("data") shouldBe "data-456"
        result.meta?.get("correlationKey") shouldBe "data-456" // Uses data as fallback
        result.meta?.get("suffix") shouldBe "sfx"
        result.meta?.get("correlation") shouldBe "false"
    }

    @Test
    fun `normalize should extract metadata correctly for CORREL events`() {
        // Arrange
        val eventPublisher = EventPublisherInMemory()
        val pipeline = EvaluatePipeline(
            id="test-id",
            envConfig = envConfig,
            eventPublisher = eventPublisher,
            eventsMicrostore = EventsMicrostoreInMemory(),
            )
        val eventAsString = """{"id": "ev2", "type": "CorrelEvent"}"""
        val rawNewMap = mapOf(
            "event" to EventAV().withS(eventAsString),
            "discriminator" to EventAV().withS("CORREL"),
            "pk" to EventAV().withS("pk-789"),
            "data" to EventAV().withS("data-012")
        )
        val uow = UnitOfWork(
            event = object : Event {
                override var id: String? = "ev2"
                override var timestamp: Long? = null
                override var partitionKey: String? = null
                override var tags: Map<String, String>? = null
                override var raw: Any? = RecordPair(new = RecordImage(rawNewMap), old = null)
                override var eem: Any? = null
                override fun eventType() = "CorrelEvent"
                override fun encoded() = ""
            },
            record = DynamodbEvent.DynamodbStreamRecord().apply {
                dynamodb = StreamRecord().apply { sequenceNumber = "seq-888" }
            }
        )

        // Act
        val result = pipeline.normalize(uow)

        // Assert
        result.event.shouldBeInstanceOf<JsonEvent>()
        result.event.id shouldBe "ev2"
        result.meta?.get("id") shouldBe "ev2"
        result.meta?.get("sequenceNumber") shouldBe "seq-888"
        result.meta?.get("pk") shouldBe "pk-789"
        result.meta?.get("correlationKey") shouldBe "pk-789" // Uses pk for CORREL
        result.meta?.get("correlation") shouldBe "true"
    }

    @Test
    fun `normalize should handle missing fields and empty raw data gracefully`() {
        // Arrange
        val eventPublisher = EventPublisherInMemory()
        val pipeline = EvaluatePipeline(
            id="test-id",
            envConfig = envConfig,
            eventPublisher = eventPublisher,
            eventsMicrostore = EventsMicrostoreInMemory(),
            )
        val uowEmpty = UnitOfWork(
            event = object : Event {
                override var id: String? = null
                override var timestamp: Long? = null
                override var partitionKey: String? = null
                override var tags: Map<String, String>? = null
                override var raw: Any? = null
                override var eem: Any? = null
                override fun eventType() = "Unknown"
                override fun encoded() = ""
            },
            record = null
        )

        // Act
        val resultEmpty = pipeline.normalize(uowEmpty)

        // Assert
        resultEmpty.event.shouldBeInstanceOf<JsonEvent>() // default "{}" parses to an empty JSON
        resultEmpty.meta?.get("id") shouldBe null
        resultEmpty.meta?.get("sequenceNumber") shouldBe null
        resultEmpty.meta?.get("ttl") shouldBe "null"
        resultEmpty.meta?.get("expire") shouldBe "null"
        resultEmpty.meta?.get("pk") shouldBe null
        resultEmpty.meta?.get("data") shouldBe null
        resultEmpty.meta?.get("correlationKey") shouldBe null
        resultEmpty.meta?.get("suffix") shouldBe null
        resultEmpty.meta?.get("correlation") shouldBe "false"
    }

    @Test
    fun `forEvents should return true for valid INSERT and CORREL events, and false otherwise`() {
        // Arrange
        val eventPublisher = EventPublisherInMemory()
        val pipeline = EvaluatePipeline(
            id="test-id",
            envConfig = envConfig,
            eventPublisher = eventPublisher,
            eventsMicrostore = EventsMicrostoreInMemory(),
            )
        
        val insertEventUow = UnitOfWork(
            record = DynamodbEvent.DynamodbStreamRecord().apply {
                eventName = "INSERT"
                dynamodb = StreamRecord().apply {
                    keys = mapOf("sk" to EventAV().withS("EVENT"))
                }
            }
        )

        val correlEventUow = UnitOfWork(
            record = DynamodbEvent.DynamodbStreamRecord().apply {
                eventName = "MODIFY"
                dynamodb = StreamRecord().apply {
                    newImage = mapOf("discriminator" to EventAV().withS("CORREL"))
                }
            }
        )

        val otherEventUow = UnitOfWork(
            record = DynamodbEvent.DynamodbStreamRecord().apply {
                eventName = "MODIFY"
            }
        )

        // Act & Assert
        pipeline.forEvents(insertEventUow) shouldBe true
        pipeline.forEvents(correlEventUow) shouldBe true
        pipeline.forEvents(otherEventUow) shouldBe false
        pipeline.forEvents(UnitOfWork()) shouldBe false
    }

    @Test
    fun `defaultUnmarshall should use custom unmarshall function or fallback to JsonEvent`() {
        // Arrange
        val customEvent = object : Event {
            override var id: String? = "1"
            override var timestamp: Long? = 123
            override var partitionKey: String? = "pk"
            override var tags: Map<String, String>? = emptyMap()
            override var raw: Any? = null
            override var eem: Any? = null
            override fun eventType() = "custom"
            override fun encoded() = ""
        }
        val eventPublisher = EventPublisherInMemory()
        val pipelineWithUnmarshall = EvaluatePipeline(
            id = "test-id",
            envConfig = envConfig,
            eventPublisher = eventPublisher,
            unmarshall = { customEvent },
            eventsMicrostore = EventsMicrostoreInMemory(),
            )
        val pipelineWithoutUnmarshall = EvaluatePipeline(
            id="test-id",
            envConfig = envConfig,
            eventPublisher = eventPublisher,
            eventsMicrostore = EventsMicrostoreInMemory(),
            )

        val jsonString = """{"id": "2", "type": "test"}"""

        // Act
        val resultWithUnmarshall = pipelineWithUnmarshall.defaultUnmarshall(jsonString)
        val resultWithoutUnmarshall = pipelineWithoutUnmarshall.defaultUnmarshall(jsonString)

        // Assert
        resultWithUnmarshall shouldBe customEvent
        
        resultWithoutUnmarshall.shouldBeInstanceOf<JsonEvent>()
        resultWithoutUnmarshall.id shouldBe "2"
        resultWithoutUnmarshall.eventType() shouldBe "test"
    }

    @Test
    fun `onCorrelationKeySuffix should evaluate rules against matching and non-matching suffixes`() {
        // Arrange
        val eventPublisher = EventPublisherInMemory()
        val pipelineNoSuffix = EvaluatePipeline(
            id="test",
            envConfig = envConfig,
            eventPublisher = eventPublisher,
            correlationKeySuffix = "",
            eventsMicrostore = EventsMicrostoreInMemory(),
            )
        val pipelineWithSuffix = EvaluatePipeline(
            id="test",
            envConfig = envConfig,
            eventPublisher = eventPublisher,
            correlationKeySuffix = "expectedSuffix",
            eventsMicrostore = EventsMicrostoreInMemory(),
            )

        val uowNoSuffix = UnitOfWork(meta = mapOf())
        val uowEmptySuffix = UnitOfWork(meta = mapOf("suffix" to ""))
        val uowMatchingSuffix = UnitOfWork(meta = mapOf("suffix" to "expectedSuffix"))
        val uowDifferentSuffix = UnitOfWork(meta = mapOf("suffix" to "otherSuffix"))

        // Act & Assert
        pipelineNoSuffix.onCorrelationKeySuffix(uowNoSuffix) shouldBe true
        pipelineNoSuffix.onCorrelationKeySuffix(uowEmptySuffix) shouldBe true
        pipelineNoSuffix.onCorrelationKeySuffix(uowDifferentSuffix) shouldBe false

        pipelineWithSuffix.onCorrelationKeySuffix(uowNoSuffix) shouldBe false
        pipelineWithSuffix.onCorrelationKeySuffix(uowEmptySuffix) shouldBe false
        pipelineWithSuffix.onCorrelationKeySuffix(uowMatchingSuffix) shouldBe true
        pipelineWithSuffix.onCorrelationKeySuffix(uowDifferentSuffix) shouldBe false
    }

    @Test
    fun `toQueryRequest should create appropriate QueryRequest based on correlation flag`() {
        // Arrange
        val eventPublisher = EventPublisherInMemory()
        val pipeline = EvaluatePipeline(
            id="test",
            envConfig = envConfig,
            eventPublisher = eventPublisher,
            eventsMicrostore = EventsMicrostoreInMemory(),
            index = "CustomIndex")

        val correlUow = UnitOfWork(meta = mapOf("correlation" to "true", "pk" to "test-pk"))
        val dataUow = UnitOfWork(meta = mapOf("correlation" to "false", "data" to "test-data"))
        val nullCorrelUow = UnitOfWork(meta = mapOf("data" to "test-data-2"))

        // Act
        val resultCorrel = pipeline.toQueryRequest(correlUow)
        val resultData = pipeline.toQueryRequest(dataUow)
        val resultNullCorrel = pipeline.toQueryRequest(nullCorrelUow)

        // Assert
        resultCorrel.queryRequest.shouldNotBeNull()
        resultCorrel.queryRequest.keyConditionExpression shouldBe "#pk = :pk"
        resultCorrel.queryRequest.expressionAttributeNames?.get("#pk") shouldBe "pk"
        val pkVal = resultCorrel.queryRequest.expressionAttributeValues?.get(":pk") as? SdkAV.S
        pkVal?.value shouldBe "test-pk"
        resultCorrel.queryRequest.consistentRead shouldBe true

        resultData.queryRequest.shouldNotBeNull()
        resultData.queryRequest.indexName shouldBe "CustomIndex"
        resultData.queryRequest.keyConditionExpression shouldBe "#data = :data"
        resultData.queryRequest.expressionAttributeNames?.get("#data") shouldBe "data"
        val dataVal = resultData.queryRequest.expressionAttributeValues?.get(":data") as? SdkAV.S
        dataVal?.value shouldBe "test-data"

        resultNullCorrel.queryRequest.shouldNotBeNull()
        resultNullCorrel.queryRequest.keyConditionExpression shouldBe "#data = :data"
        val nullCorrelDataVal = resultNullCorrel.queryRequest.expressionAttributeValues?.get(":data") as? SdkAV.S
        nullCorrelDataVal?.value shouldBe "test-data-2"
    }

    @Test
    fun `toHigherOrderEvents should emit expected events for basic configuration`() {
        // Arrange
        val eventPublisher = EventPublisherInMemory()
        val pipeline = EvaluatePipeline(
            id = "test-id",
            envConfig = envConfig,
            eventPublisher = eventPublisher,
            correlationKeySuffix = "suffix",
            higherOrderEmit = EmitOption.Basic("MyHigherOrderType"),
            eventsMicrostore = EventsMicrostoreInMemory(),
            )
        
        val trigger1 = object : Event {
            override var id: String? = "t1"
            override var timestamp: Long? = 100
            override var partitionKey: String? = "pk1"
            override var tags: Map<String, String>? = mapOf("tag1" to "v1", "region" to "us-east-1")
            override var raw: Any? = null
            override var eem: Any? = null
            override fun eventType() = "Type1"
            override fun encoded() = ""
        }
        
        val trigger2 = object : Event {
            override var id: String? = "t2"
            override var timestamp: Long? = 200
            override var partitionKey: String? = "pk2"
            override var tags: Map<String, String>? = mapOf("tag2" to "v2", "source" to "aws")
            override var raw: Any? = null
            override var eem: Any? = null
            override fun eventType() = "Type2"
            override fun encoded() = ""
        }

        val originalEvent = object : Event {
            override var id: String? = "ev1"
            override var timestamp: Long? = 300
            override var partitionKey: String? = "pk3"
            override var tags: Map<String, String>? = null
            override var raw: Any? = "raw"
            override var eem: Any? = "eem"
            override fun eventType() = "Orig"
            override fun encoded() = ""
        }

        val uow = UnitOfWork(
            meta = mapOf(
                "id" to "metaId",
                "correlationKey" to "corrKey.suffix"
            ),
            triggers = listOf(trigger1, trigger2),
            event = originalEvent
        )

        // Act
        val result = pipeline.toHigherOrderEvents(uow)

        // Assert
        result.size shouldBe 1
        val newUow = result.first()
        val emittedEvent = newUow.event as EvaluatePipeline.HigherOrderEvent
        
        emittedEvent.id shouldBe "metaId.test-id"
        emittedEvent.type shouldBe "MyHigherOrderType"
        emittedEvent.timestamp shouldBe 200
        emittedEvent.partitionKey shouldBe "corrKey"
        emittedEvent.tags shouldBe mapOf("tag1" to "v1", "tag2" to "v2")
        emittedEvent.mappedTriggers?.size shouldBe 2
        emittedEvent.mappedTriggers?.get(0) shouldBe mapOf("id" to "t1", "type" to "Type1", "timestamp" to 100L)
        emittedEvent.mappedTriggers?.get(1) shouldBe mapOf("id" to "t2", "type" to "Type2", "timestamp" to 200L)
        emittedEvent.baseEvent shouldBe originalEvent
        emittedEvent.raw shouldBe "raw"
        emittedEvent.eem shouldBe "eem"
    }

    @Test
    fun `toHigherOrderEvents should emit via custom function if provided`() {
        // Arrange
        val customEvent = object : Event {
            override var id: String? = "custom1"
            override var timestamp: Long? = null
            override var partitionKey: String? = null
            override var tags: Map<String, String>? = null
            override var raw: Any? = null
            override var eem: Any? = null
            override fun eventType() = "Custom"
            override fun encoded() = ""
        }

        val emitFunction: (UnitOfWork, Event) -> List<Event> = { _, _ -> listOf(customEvent) }
        val eventPublisher = EventPublisherInMemory()
        val pipeline = EvaluatePipeline(
            id = "test-id",
            envConfig = envConfig,
            eventPublisher = eventPublisher,
            higherOrderEmit = EmitOption.Custom(emitFunction),
            eventsMicrostore = EventsMicrostoreInMemory(),
            )

        val uow = UnitOfWork(meta = mapOf("id" to "m1", "correlationKey" to "k1"))

        // Act
        val result = pipeline.toHigherOrderEvents(uow)

        // Assert
        result.size shouldBe 1
        result.first().event shouldBe customEvent
    }

    @Test
    fun `connect should successfully process valid UnitOfWork and filter out invalid ones`() : Unit = runBlocking {
        // Arrange
        val eventPublisher = EventPublisherInMemory()
        val pipeline = EvaluatePipeline(
            id = "test-pipeline",
            envConfig = envConfig,
            eventPublisher = eventPublisher,
            eventsMicrostore = EventsMicrostoreInMemory(),
            // Required to avoid IllegalArgumentException during toHigherOrderEvents
            higherOrderEmit = EmitOption.Basic("MyHigherOrderType")
        )
        val faultManager = FaultManager(envConfig = envConfig, eventPublisher = EventPublisherInMemory())

        val eventAsString = """{"id": "ev1", "type": "TestEvent"}"""
        val rawNewMap = mapOf(
            "event" to EventAV().withS(eventAsString),
            "discriminator" to EventAV().withS("EVENT"),
            "pk" to EventAV().withS("pk-123"),
            "data" to EventAV().withS("data-456")
        )

        // A valid UnitOfWork that meets the `forEvents` and `onContentType` criteria
        val validUow = UnitOfWork(
            pipeline = pipeline,
            event = object : Event {
                override var id: String? = "ev1"
                override var timestamp: Long? = 123456789L
                override var partitionKey: String? = null
                override var tags: Map<String, String>? = null
                override var raw: Any? = RecordPair(new = RecordImage(rawNewMap), old = null)
                override var eem: Any? = null
                override fun eventType() = "TestEvent"
                override fun encoded() = ""
            },
            record = DynamodbEvent.DynamodbStreamRecord().apply {
                eventName = "INSERT"
                dynamodb = StreamRecord().apply {
                    keys = mapOf("sk" to EventAV().withS("EVENT"))
                    sequenceNumber = "seq-999"
                }
            }
        )

        // An invalid UnitOfWork that should be filtered out early by `forEvents`
        val invalidUow = UnitOfWork(
            record = DynamodbEvent.DynamodbStreamRecord().apply {
                eventName = "MODIFY" 
            }
        )

        val uowFlow = kotlinx.coroutines.flow.flowOf(validUow, invalidUow)

        // Act
        val results = pipeline.connect(faultManager, uowFlow).toList()

        // Assert
        results.size shouldBe 1
        
        val processedUow = results.first()
        processedUow.shouldNotBeNull()
        
        // Assert higher order event mapping behavior
        val emittedEvent = processedUow.event
        emittedEvent.shouldBeInstanceOf<EvaluatePipeline.HigherOrderEvent>()
        emittedEvent.type shouldBe "MyHigherOrderType"
        emittedEvent.id shouldBe "ev1.test-pipeline"
        
        emittedEvent.mappedTriggers.shouldNotBeNull()
        emittedEvent.mappedTriggers!!.size shouldBe 1
        emittedEvent.mappedTriggers!![0]["id"] shouldBe "ev1"
        
        // Assert normalization mapped variables onto meta
        processedUow.meta?.get("sequenceNumber") shouldBe "seq-999"
        processedUow.meta?.get("pk") shouldBe "pk-123"
        processedUow.meta?.get("data") shouldBe "data-456"
    }
}