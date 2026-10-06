package io.kopipes.aws

import io.kopipes.aws.sinks.EventBridgePublisher
import io.kopipes.core.GlobalRegistry
import io.kopipes.core.faults.FaultManager
import io.kopipes.core.sinks.EventPublisher
import io.kopipes.core.sinks.EventPublisherInMemory
import io.kotest.matchers.nulls.shouldNotBeNull
import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.shouldBeInstanceOf
import io.mockk.mockk
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

class AwsGlobalRegistryTest {

    @BeforeEach
    fun setUp() {
        AwsGlobalRegistry.reset()
    }

    @Test
    fun `envConfig should default to AwsEnvironmentConfig`() {
        val config = AwsGlobalRegistry.envConfig()
        config.shouldNotBeNull()
        config.shouldBeInstanceOf<AwsEnvironmentConfig>()
    }

    @Test
    fun `eventPublisher should default to EventBridgePublisher`() {
        val publisher = AwsGlobalRegistry.eventPublisher()
        publisher.shouldNotBeNull()
        publisher.shouldBeInstanceOf<EventBridgePublisher>()
    }

    @Test
    fun `faultManager should default to FaultManager with EventBridgePublisher`() {
        val fm = AwsGlobalRegistry.faultManager()
        fm.shouldNotBeNull()
        fm.publisher().shouldBeInstanceOf<EventBridgePublisher>()
    }

    @Test
    fun `eventPublisher can be overridden via AwsGlobalRegistry`() {
        val customPub = mockk<EventPublisher>()
        AwsGlobalRegistry.setEventPublisher(customPub)

        AwsGlobalRegistry.eventPublisher() shouldBe customPub
        GlobalRegistry.eventPublisher() shouldBe customPub
    }

    @Test
    fun `faultManager can be overridden via AwsGlobalRegistry`() {
        val customFm = mockk<FaultManager>()
        AwsGlobalRegistry.setFaultManager(customFm)

        AwsGlobalRegistry.faultManager() shouldBe customFm
        GlobalRegistry.faultManager() shouldBe customFm
    }

    @Test
    fun `reset should restore AWS default factories`() {
        val customPub = EventPublisherInMemory()
        AwsGlobalRegistry.setEventPublisher(customPub)

        AwsGlobalRegistry.eventPublisher() shouldBe customPub

        AwsGlobalRegistry.reset()

        val restored = AwsGlobalRegistry.eventPublisher()
        restored.shouldBeInstanceOf<EventBridgePublisher>()
    }
}
