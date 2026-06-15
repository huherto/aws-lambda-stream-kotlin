package io.github.huherto.awsLambdaStream

import aws.smithy.kotlin.runtime.SdkBaseException
import io.github.huherto.awsLambdaStream.sinks.EventPublisherInMemory
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.collections.shouldBeEmpty
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.shouldBe
import io.mockk.every
import io.mockk.mockk
import io.mockk.spyk
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test

class FaultManagerTest {

    private val envConfig = spyk<EnvironmentConfig>()

    @Test
    fun `faulty should return block result when no exception occurs`(): Unit = runBlocking {
        // Arrange
        val eventPublisher = EventPublisherInMemory()
        val faultManager = FaultManager(envConfig, eventPublisher)
        val uow = mockk<UnitOfWork>(relaxed = true)

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
        val faultManager = spyk(FaultManager(envConfig, eventPublisher))
        every { faultManager.logError(any()) } returns Unit

        val uow = UnitOfWork()
        val exception = RuntimeException("test error")

        // Act
        val result = faultManager.faulty(uow) { throw exception }

        // Assert
        result shouldBe null
        val faults = faultManager.getFaults()
        faults shouldHaveSize 1
        faults.first().failureException?.cause shouldBe exception
    }

    @Test
    fun `redirectFailure should add a fault for non-retriable exceptions when stream retry is disabled`() {
        // Arrange
        val envConfig = spyk<EnvironmentConfig>()
        every { envConfig.awsLambdaFunctionName() } returns "test-function"
        every { envConfig.streamRetryEnabled() } returns false

        val eventPublisher = EventPublisherInMemory()
        val faultManager = spyk(FaultManager(envConfig, eventPublisher))
        every { faultManager.logError(any()) } returns Unit

        val uow = mockk<UnitOfWork>(relaxed = true)
        val nonRetriableEx = FaultException(uow, RuntimeException("non-retriable"))

        // Act
        faultManager.redirectFailure(nonRetriableEx)

        // Assert
        faultManager.getFaults() shouldHaveSize 1
        faultManager.getFaults()[0].failureException shouldBe nonRetriableEx
    }

    @Test
    fun `redirectFailure should throw for retriable SDK exceptions when stream retry is enabled`() {
        // Arrange
        val envConfig = spyk<EnvironmentConfig>()
        every { envConfig.awsLambdaFunctionName() } returns "test-function"
        every { envConfig.streamRetryEnabled() } returns true

        val eventPublisher = EventPublisherInMemory()
        val faultManager = spyk(FaultManager(envConfig, eventPublisher))
        every { faultManager.logError(any()) } returns Unit

        val uow = mockk<UnitOfWork>(relaxed = true)
        val sdkException = mockk<SdkBaseException>(relaxed = true)
        every { sdkException.sdkErrorMetadata.isRetryable } returns true
        val retriableEx = FaultException(uow, sdkException)

        // Act & Assert
        shouldThrow<FaultException> {
            faultManager.redirectFailure(retriableEx)
        }
        faultManager.getFaults() shouldHaveSize 0
    }

    @Test
    fun `redirectFailure should add a fault for non-retriable SDK exceptions when stream retry is enabled`() {
        // Arrange
        val envConfig = spyk<EnvironmentConfig>()
        every { envConfig.awsLambdaFunctionName() } returns "test-function"
        every { envConfig.streamRetryEnabled() } returns true

        val eventPublisher = EventPublisherInMemory()
        val faultManager = spyk(FaultManager(envConfig, eventPublisher))
        every { faultManager.logError(any()) } returns Unit

        val uow = mockk<UnitOfWork>(relaxed = true)
        val sdkException = mockk<SdkBaseException>(relaxed = true)
        every { sdkException.sdkErrorMetadata.isRetryable } returns false
        val nonRetriableSdkEx = FaultException(uow, sdkException)

        // Act
        faultManager.redirectFailure(nonRetriableSdkEx)

        // Assert
        faultManager.getFaults() shouldHaveSize 1
        faultManager.getFaults()[0].failureException shouldBe nonRetriableSdkEx
    }

    @Test
    fun `mapNotFaulty should filter out faulty items and accumulate faults`(): Unit = runBlocking {
        // Arrange
        every { envConfig.awsLambdaFunctionName() } returns "test-function"
        every { envConfig.streamRetryEnabled() } returns false
        val eventPublisher = EventPublisherInMemory()
        val faultManager = spyk(FaultManager(envConfig, eventPublisher))
        every { faultManager.logError(any()) } returns Unit

        val uow1 = mockk<UnitOfWork>(relaxed = true)
        val uow2 = mockk<UnitOfWork>(relaxed = true)
        val uow3 = mockk<UnitOfWork>(relaxed = true)

        val flow = flowOf(uow1, uow2, uow3)

        // Act
        val resultFlow = with(faultManager) {
            flow.mapNotFaulty { uow ->
                if (uow == uow2) throw RuntimeException("error on uow2")
                uow
            }
        }
        val resultList = resultFlow.toList()

        // Assert
        resultList shouldBe listOf(uow1, uow3)
        val faults = faultManager.getFaults()
        faults shouldHaveSize 1
        faults[0].failureException?.cause?.message shouldBe "error on uow2"
    }

    @Test
    fun `flushFaults should map accumulated faults to UnitOfWork and pass them to publishFlow`(): Unit = runBlocking {
        // Arrange
        every { envConfig.awsLambdaFunctionName() } returns "test-function"
        every { envConfig.streamRetryEnabled() } returns false

        val eventPublisher = EventPublisherInMemory()
        val faultManager = spyk(FaultManager(envConfig, eventPublisher))
        every { faultManager.logError(any()) } returns Unit

        val uow = mockk<UnitOfWork>(relaxed = true)

        faultManager.faulty(uow) { throw RuntimeException("error 1") }
        faultManager.faulty(uow) { throw RuntimeException("error 2") }

        faultManager.getFaults() shouldHaveSize 2

        // Act
        faultManager.flushFaults()

        // Assert
        faultManager.getFaults().shouldBeEmpty()

        val emittedList = eventPublisher.events()
        emittedList shouldHaveSize 2
        (emittedList[0] is FaultEvent) shouldBe true
        (emittedList[0] as FaultEvent).failureException?.cause?.message shouldBe "error 1"
        (emittedList[1] as FaultEvent).failureException?.cause?.message shouldBe "error 2"
    }

    @Test
    fun `logError should not throw exceptions`() {
        // Arrange
        val eventPublisher = EventPublisherInMemory()
        val faultManager = FaultManager(envConfig, eventPublisher)

        // Act & Assert
        // Ensuring it doesn't crash the execution
        faultManager.logError(RuntimeException("Test exception"))
    }
}