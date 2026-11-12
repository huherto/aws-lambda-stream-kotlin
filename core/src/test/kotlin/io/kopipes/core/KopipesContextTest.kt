package io.kopipes.core

import io.kopipes.core.sinks.EventPublisherInMemory
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.shouldBeInstanceOf
import io.kotest.matchers.types.shouldBeSameInstanceAs
import io.mockk.every
import io.mockk.spyk
import org.junit.jupiter.api.Test

class KopipesContextTest {

    private class Retryable : RuntimeException("retry me")

    @Test
    fun `defaults wire an in-memory publisher into the fault manager`() {
        // arrange & act
        val context = BasicKopipesContext()

        // assert
        context.eventPublisher.shouldBeInstanceOf<EventPublisherInMemory>()
        context.faultManager.publisher() shouldBeSameInstanceAs context.eventPublisher
        context.faultManager.unitOfWorkSnapshotter shouldBeSameInstanceAs context.unitOfWorkSnapshotter
    }

    @Test
    fun `a replaced publisher is the one the default fault manager uses`() {
        // arrange
        val publisher = EventPublisherInMemory()

        // act
        val context = BasicKopipesContext(eventPublisher = publisher)

        // assert
        context.faultManager.publisher() shouldBeSameInstanceAs publisher
    }

    @Test
    fun `the fault manager takes its retry switches from the config and the retry check from the context`() {
        // arrange
        val envConfig = spyk<EnvironmentConfig>()
        every { envConfig.streamRetryEnabled() } returns true
        val context = BasicKopipesContext(envConfig = envConfig, isRetryable = { it is Retryable })
        val fm = context.faultManager

        // act
        val retried = shouldThrow<FaultException> { fm.redirectFailure(FaultException(UnitOfWork(), Retryable())) }
        fm.redirectFailure(FaultException(UnitOfWork(), IllegalStateException("not retryable")))

        // assert
        retried.cause.shouldBeInstanceOf<Retryable>()
        fm.getFaults() shouldHaveSize 1
    }

    @Test
    fun `two contexts share nothing`() {
        // arrange & act
        val first = BasicKopipesContext()
        val second = BasicKopipesContext()

        // assert
        (first.faultManager === second.faultManager) shouldBe false
    }
}
