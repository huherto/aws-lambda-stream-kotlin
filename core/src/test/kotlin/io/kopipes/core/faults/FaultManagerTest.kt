package io.kopipes.core.faults

import io.kopipes.core.EnvironmentConfig
import io.kopipes.core.FaultException
import io.kopipes.core.GlobalRegistry
import io.kopipes.core.UnitOfWork
import io.kopipes.core.sinks.EventPublisherInMemory
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.collections.shouldBeEmpty
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.shouldBe
import io.mockk.every
import io.mockk.spyk
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test

class FaultManagerTest {

    class TestRetryableException(message: String) : RuntimeException(message)

    @Test
    fun `faulty should return block result when no exception occurs`(): Unit = runBlocking {
        // Arrange
        val eventPublisher = EventPublisherInMemory()
        val faultManager = FaultManager(eventPublisher)
        val uow = UnitOfWork()

        // Act
        val result = faultManager.faulty(uow) { "success" }

        // Assert
        result shouldBe "success"
        faultManager.getFaults().shouldBeEmpty()
    }

    @Test
    fun `faulty should catch exception, redirect failure, and return null`(): Unit = runBlocking {
        // Arrange
        val envConfig = spyk<EnvironmentConfig>()
        every { envConfig.awsLambdaFunctionName() } returns "test-function"
        every { envConfig.streamRetryEnabled() } returns false

        val eventPublisher = EventPublisherInMemory()
        val faultManager = spyk(FaultManager(eventPublisher))
        every { faultManager.logError(any()) } returns Unit

        val uow = UnitOfWork()
        val exception = RuntimeException("test error")

        // Act
        val result = faultManager.faulty(uow) { throw exception }

        // Assert
        result shouldBe null
        val faults = faultManager.getFaults()
        faults shouldHaveSize 1
        faults.first().err?.message shouldBe "java.lang.RuntimeException: test error"
    }

    @Test
    fun `redirectFailure should add a fault for non-retriable exceptions when stream retry is disabled`() {
        // Arrange
        val envConfig = spyk<EnvironmentConfig>()
        every { envConfig.awsLambdaFunctionName() } returns "test-function"
        every { envConfig.streamRetryEnabled() } returns false

        val eventPublisher = EventPublisherInMemory()
        val faultManager = spyk(FaultManager(eventPublisher))
        every { faultManager.logError(any()) } returns Unit

        val uow = UnitOfWork()
        val nonRetriableEx = FaultException(uow, RuntimeException("non-retriable"))

        // Act
        faultManager.redirectFailure(nonRetriableEx)

        // Assert
        faultManager.getFaults() shouldHaveSize 1
        faultManager.getFaults()[0].err?.message shouldBe nonRetriableEx.message
    }

    @Test
    fun `redirectFailure should throw for retriable exceptions when stream retry is enabled`() {
        // Arrange
        val envConfig = spyk<EnvironmentConfig>()
        every { envConfig.awsLambdaFunctionName() } returns "test-function"
        every { envConfig.streamRetryEnabled() } returns true
        GlobalRegistry.setEnvConfig(envConfig)

        val eventPublisher = EventPublisherInMemory()
        val faultManager = spyk(FaultManager(eventPublisher, isRetryable = { it is TestRetryableException }))
        every { faultManager.logError(any()) } returns Unit

        val uow = UnitOfWork()
        val retriableEx = FaultException(uow, TestRetryableException("retriable"))

        // Act & Assert
        shouldThrow<FaultException> {
            faultManager.redirectFailure(retriableEx)
        }
        faultManager.getFaults() shouldHaveSize 0
    }

    @Test
    fun `redirectFailure should add a fault for non-retriable exceptions when stream retry is enabled`() {
        // Arrange
        val envConfig = spyk<EnvironmentConfig>()
        every { envConfig.awsLambdaFunctionName() } returns "test-function"
        every { envConfig.streamRetryEnabled() } returns true
        GlobalRegistry.setEnvConfig(envConfig)

        val eventPublisher = EventPublisherInMemory()
        val faultManager = spyk(FaultManager(eventPublisher, isRetryable = { it is TestRetryableException }))
        every { faultManager.logError(any()) } returns Unit

        val uow = UnitOfWork()
        val nonRetriableEx = FaultException(uow, RuntimeException("non-retriable"))

        // Act
        faultManager.redirectFailure(nonRetriableEx)

        // Assert
        faultManager.getFaults() shouldHaveSize 1
    }

    @Test
    fun `redirectFailure should store uow for item-level retry when enabled`() {
        // Arrange
        val envConfig = spyk<EnvironmentConfig>()
        every { envConfig.awsLambdaFunctionName() } returns "test-function"
        every { envConfig.streamRetryEnabled() } returns true
        every { envConfig.itemLevelRetryEnabled() } returns true
        GlobalRegistry.setEnvConfig(envConfig)

        val eventPublisher = EventPublisherInMemory()
        val faultManager = spyk(FaultManager(eventPublisher, isRetryable = { it is TestRetryableException }))
        every { faultManager.logError(any()) } returns Unit

        val uow = UnitOfWork(sequenceNumber = "seq-123")
        val retriableEx = FaultException(uow, TestRetryableException("retriable"))

        // Act
        faultManager.redirectFailure(retriableEx)

        // Assert
        faultManager.getFaults() shouldHaveSize 0
        val retryable = faultManager.pollRetryableItems()
        retryable shouldHaveSize 1
        retryable.first().sequenceNumber shouldBe "seq-123"
    }

    @Test
    fun `flushFaults should publish all queued faults and return count`(): Unit = runBlocking {
        // Arrange
        val eventPublisher = EventPublisherInMemory()
        val faultManager = spyk(FaultManager(eventPublisher))
        every { faultManager.logError(any()) } returns Unit

        val uow = UnitOfWork()
        faultManager.redirectFailure(FaultException(uow, RuntimeException("error 1")))
        faultManager.redirectFailure(FaultException(uow, RuntimeException("error 2")))

        // Act
        val count = faultManager.flushFaults()

        // Assert
        count shouldBe 2
        faultManager.getFaults().shouldBeEmpty()
        eventPublisher.getUows() shouldHaveSize 2
    }

    @Test
    fun `mapNotFaulty should process valid items and redirect faulty ones`(): Unit = runBlocking {
        // Arrange
        val eventPublisher = EventPublisherInMemory()
        val faultManager = spyk(FaultManager(eventPublisher))
        every { faultManager.logError(any()) } returns Unit

        val uow1 = UnitOfWork(key = "1")
        val uow2 = UnitOfWork(key = "2")
        val uow3 = UnitOfWork(key = "3")

        val flow = flowOf(uow1, uow2, uow3)

        // Act
        val results = with(faultManager) {
            flow.mapNotFaulty { uow ->
                if (uow.key == "2") throw RuntimeException("error on 2")
                "processed-${uow.key}"
            }.toList()
        }

        // Assert
        results shouldBe listOf("processed-1", "processed-3")
        faultManager.getFaults() shouldHaveSize 1
    }

    @Test
    fun `filterNotFaulty should keep matching items and redirect faulty ones`(): Unit = runBlocking {
        // Arrange
        val eventPublisher = EventPublisherInMemory()
        val faultManager = spyk(FaultManager(eventPublisher))
        every { faultManager.logError(any()) } returns Unit

        val uow1 = UnitOfWork(key = "1")
        val uow2 = UnitOfWork(key = "2")
        val uow3 = UnitOfWork(key = "3")

        val flow = flowOf(uow1, uow2, uow3)

        // Act
        val results = with(faultManager) {
            flow.filterNotFaulty { uow ->
                if (uow.key == "2") throw RuntimeException("error on 2")
                uow.key == "1"
            }.toList()
        }

        // Assert
        results shouldBe listOf(uow1)
        faultManager.getFaults() shouldHaveSize 1
    }
}
