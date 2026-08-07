package io.github.huherto.awsLambdaStream

import io.github.huherto.awsLambdaStream.sinks.EventPublisher
import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNotBe
import io.mockk.mockk
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

class GlobalRegistryTest {

    class TestEnvironmentConfig : EnvironmentConfig() {
        override fun serializationStrategy(): String? = "jackson"
        override fun awsLambdaFunctionName(): String? = "test-function"
    }

    @BeforeEach
    fun setUp() {
        GlobalRegistry.reset()
        // Provide a stable environment config for components that need it during initialization
        GlobalRegistry.setEnvConfig(TestEnvironmentConfig())
    }

    @Test
    fun `envConfig should be lazy and return singleton`() {
        val config1 = GlobalRegistry.envConfig()
        val config2 = GlobalRegistry.envConfig()
        
        config1 shouldBe config2
    }

    @Test
    fun `envConfig default should be EnvironmentConfig`() {
        GlobalRegistry.reset()
        GlobalRegistry.envConfig() shouldNotBe null
    }

    @Test
    fun `envConfig can be overridden`() {
        val customConfig = object : EnvironmentConfig() {}
        GlobalRegistry.setEnvConfig(customConfig)
        
        GlobalRegistry.envConfig() shouldBe customConfig
    }

    @Test
    fun `eventPublisher should be lazy and return singleton`() {
        val pub1 = GlobalRegistry.eventPublisher()
        val pub2 = GlobalRegistry.eventPublisher()
        
        pub1 shouldBe pub2
    }

    @Test
    fun `eventPublisher can be overridden`() {
        val customPub = mockk<EventPublisher>()
        GlobalRegistry.setEventPublisher(customPub)
        
        GlobalRegistry.eventPublisher() shouldBe customPub
    }

    @Test
    fun `faultManager should be lazy and return singleton`() {
        val fm1 = GlobalRegistry.faultManager()
        val fm2 = GlobalRegistry.faultManager()
        
        fm1 shouldBe fm2
    }

    @Test
    fun `faultManager can be overridden`() {
        val customFm = mockk<FaultManager>()
        GlobalRegistry.setFaultManager(customFm)
        
        GlobalRegistry.faultManager() shouldBe customFm
    }

    @Test
    fun `reset should clear all cached instances`() {
        val config1 = GlobalRegistry.envConfig()
        val pub1 = GlobalRegistry.eventPublisher()
        val fm1 = GlobalRegistry.faultManager()
        
        GlobalRegistry.reset()
        // We must re-set a valid config after reset, otherwise the next 
        // calls to pub/fm will fail during default initialization
        GlobalRegistry.setEnvConfig(TestEnvironmentConfig())
        
        GlobalRegistry.envConfig() shouldNotBe config1
        GlobalRegistry.eventPublisher() shouldNotBe pub1
        GlobalRegistry.faultManager() shouldNotBe fm1
    }

    @Test
    fun `faultManager uses registered envConfig and eventPublisher`() {
        val customConfig = object : EnvironmentConfig() {
            override fun awsLambdaFunctionName(): String? = "custom-function"
        }
        val customPub = mockk<EventPublisher>()
        
        GlobalRegistry.setEnvConfig(customConfig)
        GlobalRegistry.setEventPublisher(customPub)
        
        val fm = GlobalRegistry.faultManager()
        
        fm.envConfig shouldBe customConfig
        fm.publisher() shouldBe customPub
    }
}
