package io.github.huherto.awsLambdaStream

import aws.smithy.kotlin.runtime.SdkBaseException
import io.github.huherto.awsLambdaStream.sinks.EventPublisherInMemory
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.collections.shouldBeEmpty
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.shouldBe
import io.mockk.every
import io.mockk.mockk
import io.mockk.spyk
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.toList

class FaultManagerTest : FunSpec({

    val envConfig = spyk<EnvironmentConfig>()

    test("faulty should return block result when no exception occurs") {
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

    test("faulty should catch exception, redirect failure, and return null") {
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

    test("redirectFailure should handle retriable and non-retriable exceptions correctly") {
        // Arrange
        val eventPublisher = EventPublisherInMemory()
        val faultManager = spyk(FaultManager(envConfig, eventPublisher))
        every { faultManager.logError(any()) } returns Unit
        
        val uow = mockk<UnitOfWork>(relaxed = true)
        every { envConfig.awsLambdaFunctionName() } returns "test-function"
        
        // Act & Assert - Scenario 1: non-retriable (streamRetryEnabled = false)
        every { envConfig.streamRetryEnabled() } returns false
        val nonRetriableEx = FailureException(uow, RuntimeException("non-retriable"))
        
        faultManager.redirectFailure(nonRetriableEx)
        
        faultManager.getFaults() shouldHaveSize 1
        faultManager.getFaults()[0].failureException shouldBe nonRetriableEx
        
        // Act & Assert - Scenario 2: retriable SDK exception
        every { envConfig.streamRetryEnabled() } returns true
        val sdkException = mockk<SdkBaseException>(relaxed = true)
        every { sdkException.sdkErrorMetadata.isRetryable } returns true
        val retriableEx = FailureException(uow, sdkException)
        
        shouldThrow<FailureException> {
            faultManager.redirectFailure(retriableEx)
        }
        faultManager.getFaults() shouldHaveSize 1 // No new faults added
        
        // Act & Assert - Scenario 3: non-retriable SDK exception (retryable=false)
        every { sdkException.sdkErrorMetadata.isRetryable } returns false
        val nonRetriableSdkEx = FailureException(uow, sdkException)
        
        faultManager.redirectFailure(nonRetriableSdkEx)
        
        faultManager.getFaults() shouldHaveSize 2
        faultManager.getFaults()[1].failureException shouldBe nonRetriableSdkEx
    }

    test("redirectFailure should handle null AWS Lambda function name gracefully") {
        // Arrange
        val eventPublisher = EventPublisherInMemory()
        val faultManager = spyk(FaultManager(envConfig, eventPublisher))
        every { faultManager.logError(any()) } returns Unit
        
        val uow = mockk<UnitOfWork>(relaxed = true)
        
        every { envConfig.awsLambdaFunctionName() } returns null
        every { envConfig.streamRetryEnabled() } returns false
        
        val ex = FailureException(uow, RuntimeException("error"))
        
        // Act
        faultManager.redirectFailure(ex)
        
        // Assert
        val faults = faultManager.getFaults()
        faults shouldHaveSize 1
        faults.first().tags?.get("functionname") shouldBe "undefined"
    }

    test("mapNotFaulty should filter out faulty items and accumulate faults") {
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

    test("flushFaults should map accumulated faults to UnitOfWork and pass them to publishFlow") {
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
        (emittedList[0] is FailureEvent) shouldBe true
        (emittedList[0] as FailureEvent).failureException?.cause?.message shouldBe "error 1"
        (emittedList[1] as FailureEvent).failureException?.cause?.message shouldBe "error 2"
    }

    test("logError should not throw exceptions") {
        // Arrange
        val eventPublisher = EventPublisherInMemory()
        val faultManager = FaultManager(envConfig, eventPublisher)
        
        // Act & Assert
        // Ensuring it doesn't crash the execution
        faultManager.logError(RuntimeException("Test exception"))
    }

})