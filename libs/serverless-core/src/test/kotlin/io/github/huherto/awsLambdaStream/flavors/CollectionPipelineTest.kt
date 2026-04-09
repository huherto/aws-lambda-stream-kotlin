package io.github.huherto.awsLambdaStream.flavors

import io.github.huherto.awsLambdaStream.EnvironmentConfig
import io.github.huherto.awsLambdaStream.Event
import io.github.huherto.awsLambdaStream.UnitOfWork
import io.github.huherto.awsLambdaStream.sinks.EventsMicrostore
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

class CollectionPipelineTest {

    private val envConfig = spyk<EnvironmentConfig> {
        every { awsRegion() } returns "eu-west-1"
    }

    private val eventsMicrostore = mockk<EventsMicrostore>()

    private fun createPipeline(
        pipelineId: String = "pipeline-1",
        ttlDays: Int? = null,
        includeRaw: Boolean = true,
        expire: Boolean? = null,
    ): CollectPipeline {
        return CollectPipeline(
            pipelineId = pipelineId,
            envConfig = envConfig,
            eventsMicrostore = eventsMicrostore,
            ttlDays = ttlDays,
            includeRaw = includeRaw,
            expire = expire,
        )
    }

    private fun createEvent(
        id: String = "event-1",
        timestamp: Long = 1_700_000_000_000L,
        partitionKey: String? = "partition-1",
    ): Event = object : Event {
        override var id: String? = id
        override var timestamp: Long? = timestamp
        override var partitionKey: String? = partitionKey
        override var tags: Map<String, String>? = null
        override var raw: Any? = null
        override var eem: Any? = null
        override fun eventType() = "TestEvent"
        override fun encoded() = """{"id":"$id"}"""
    }

    @Test
    fun `daysInSecs should convert days to seconds for common values`() {
        // Arrange
        val pipeline = createPipeline()

        // Act
        val zeroDays = pipeline.daysInSecs(0)
        val oneDay = pipeline.daysInSecs(1)
        val twoDays = pipeline.daysInSecs(2)

        // Assert
        zeroDays shouldBe 0L
        oneDay shouldBe 86_400L
        twoDays shouldBe 172_800L
    }

    @Test
    fun `save should map UnitOfWork to SaveOptions using explicit ttl includeRaw and expire values`() : Unit = runBlocking {
        // Arrange
        val pipeline = createPipeline(
            pipelineId = "my-pipeline",
            ttlDays = 2,
            includeRaw = false,
            expire = true,
        )
        val event = createEvent(timestamp = 1_700_000_000_000L)
        val uow = UnitOfWork(
            event = event,
            key = "correlation-key",
            sequenceNumber = "seq-1",
        )

        every { eventsMicrostore.save(any()) } answers { firstArg() }

        // Act
        val result = pipeline.run {
            flowOf(uow).save().toList()
        }

        // Assert
        result shouldHaveSize 1
        val saved = result.first()
        val options = saved.saveOptions.shouldNotBeNull()

        options.pk shouldBe "event-1"
        options.sk shouldBe "EVENT"
        options.discriminator shouldBe "EVENT"
        options.timeStamp shouldBe "1700000000000"
        options.awsRegion shouldBe "eu-west-1"
        options.sequenceNumber shouldBe "seq-1"
        options.ttl shouldBe 1_700_172_800L
        options.expire shouldBe true
        options.data shouldBe "correlation-key"
        options.includeRaw shouldBe false
        options.pipelineId shouldBe "my-pipeline"
    }

    @Test
    fun `save should use env ttl when ttlDays is not provided and copy null event fields safely`() : Unit = runBlocking {
        // Arrange
        every { envConfig.ttl() } returns 5
        val pipeline = createPipeline(
            pipelineId = "pipeline-2",
            ttlDays = null,
            includeRaw = true,
            expire = null,
        )
        val uow = UnitOfWork(
            event = null,
            key = null,
            sequenceNumber = null,
        )

        every { eventsMicrostore.save(any()) } answers { firstArg() }

        // Act
        val result = pipeline.run {
            flowOf(uow).save().toList()
        }

        // Assert
        result shouldHaveSize 1
        val options = result.first().saveOptions.shouldNotBeNull()

        options.pk shouldBe null
        options.timeStamp shouldBe "null"
        options.ttl shouldBe 0L
        options.expire shouldBe null
        options.data shouldBe null
        options.includeRaw shouldBe true
        options.pipelineId shouldBe "pipeline-2"
    }

    @Test
    fun `save should delegate to eventsMicrostore and preserve the flow content`() : Unit = runBlocking {
        // Arrange
        val pipeline = createPipeline(pipelineId = "pipeline-4")
        val first = UnitOfWork(event = createEvent(id = "e-1"))
        val second = UnitOfWork(event = createEvent(id = "e-2"))

        every { eventsMicrostore.save(any()) } answers { firstArg() }

        // Act
        val result = pipeline.run {
            flowOf(first, second).save().toList()
        }

        // Assert
        result.shouldHaveSize(2)
        result[0].saveOptions.shouldNotBeNull().pk shouldBe "e-1"
        result[1].saveOptions.shouldNotBeNull().pk shouldBe "e-2"
        result[0].saveOptions.shouldNotBe(result[1].saveOptions)
    }

    @Test
    fun `save should keep saveOptions independent per element`() : Unit = runBlocking {
        // Arrange
        val pipeline = createPipeline(pipelineId = "pipeline-5")
        val uow = UnitOfWork(event = createEvent(id = "same-id"))

        every { eventsMicrostore.save(any()) } answers { firstArg() }

        // Act
        val result = pipeline.run {
            flowOf(uow).save().toList()
        }

        // Assert
        result shouldHaveSize 1
        val savedUow = result.first()
        savedUow.saveOptions.shouldNotBeNull()
        savedUow.saveOptions.shouldBeTypeOf<EventsMicrostore.SaveOptions>()
    }
}