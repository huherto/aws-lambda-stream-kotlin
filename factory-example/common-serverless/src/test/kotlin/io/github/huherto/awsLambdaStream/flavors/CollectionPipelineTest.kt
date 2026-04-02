package io.github.huherto.awsLambdaStream.flavors

import io.github.huherto.awsLambdaStream.EnvironmentConfig
import io.github.huherto.awsLambdaStream.Event
import io.github.huherto.awsLambdaStream.UnitOfWork
import io.github.huherto.awsLambdaStream.sinks.EventsMicrostoreInMemory
import io.kotest.matchers.nulls.shouldNotBeNull
import io.kotest.matchers.shouldBe
import io.mockk.every
import io.mockk.mockk
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test

class CollectionPipelineTest {

    // Helper method to create a CollectPipeline instance
    private fun createPipeline(
        ttlDays: Int? = null,
        includeRaw: Boolean = true,
        expire: String? = null,
        envConfigTtl: Int? = 33,
        eventsMicrostore: EventsMicrostoreInMemory = EventsMicrostoreInMemory()
    ): CollectPipeline {
        
        val mockEnvConfig = mockk<EnvironmentConfig>(relaxed = true) {
            every { ttl() } returns envConfigTtl
        }

        return CollectPipeline(
            pipelineId = "test-pipeline",
            envConfig = mockEnvConfig,
            ttlDays = ttlDays,
            includeRaw = includeRaw,
            expire = expire,
            eventsMicrostore = eventsMicrostore
        )
    }

    @Test
    fun `daysInSecs should calculate correct number of seconds for given days`() {
        // Arrange
        val pipeline = createPipeline()

        // Act
        val oneDay = pipeline.daysInSecs(1)
        val fiveDays = pipeline.daysInSecs(5)
        val zeroDays = pipeline.daysInSecs(0)

        // Assert
        oneDay shouldBe 86400L
        fiveDays shouldBe 432000L
        zeroDays shouldBe 0L
    }

    @Test
    fun `save internal flow extension should map UnitOfWork with saveOptions and delegate to eventsMicrostore`() :Unit = runBlocking {
        // Arrange
        val inMemoryStore = EventsMicrostoreInMemory()
        val pipeline = createPipeline(
            ttlDays = 10,
            includeRaw = false,
            expire = "2025-01-01",
            eventsMicrostore = inMemoryStore
        )

        val mockEvent = mockk<Event>(relaxed = true) {
            every { id } returns "event-123"
            every { timestamp } returns 1000000L // 1000 seconds
        }
        
        val uow = UnitOfWork(event = mockEvent)
        val inputFlow = flowOf(uow)

        // Act
        val resultFlow = with(pipeline) {
            inputFlow.save()
        }
        val resultList = resultFlow.toList()

        // Assert
        resultList.size shouldBe 1
        
        val savedUow = resultList.first()
        savedUow.saveOptions.shouldNotBeNull()
        savedUow.saveOptions?.includeRaw shouldBe false
        savedUow.saveOptions?.expire shouldBe "2025-01-01"
        
        // TTL calculation check: timestamp in secs (1000) + daysInSecs(10 days = 864000) = 865000
        savedUow.saveOptions?.ttlTimestampInSecs shouldBe 865000L

        // Verify that it was accurately recorded in the microstore via EventsMicrostoreInMemory
        val savedMap = inMemoryStore.saveUowMap()
        savedMap.size shouldBe 1
        savedMap["event-123"] shouldBe savedUow
    }

    @Test
    fun `save internal flow extension should fallback to environment config ttl if ttlDays is not explicitly provided`() : Unit = runBlocking {
        // Arrange
        val inMemoryStore = EventsMicrostoreInMemory()
        val pipeline = createPipeline(
            ttlDays = null,
            envConfigTtl = 5,
            eventsMicrostore = inMemoryStore
        )

        val mockEvent = mockk<Event>(relaxed = true) {
            every { id } returns "event-456"
            every { timestamp } returns 2000000L // 2000 seconds
        }
        
        val uow = UnitOfWork(event = mockEvent)
        val inputFlow = flowOf(uow)

        // Act
        val resultFlow = with(pipeline) {
            inputFlow.save()
        }
        val resultList = resultFlow.toList()

        // Assert
        val savedUow = resultList.first()
        
        // TTL calculation check: timestamp in secs (2000) + daysInSecs(5 days = 432000) = 434000
        savedUow.saveOptions?.ttlTimestampInSecs shouldBe 434000L
        
        // Verify it was correctly stored
        val savedMap = inMemoryStore.saveUowMap()
        savedMap.size shouldBe 1
        savedMap["event-456"] shouldBe savedUow
    }
}