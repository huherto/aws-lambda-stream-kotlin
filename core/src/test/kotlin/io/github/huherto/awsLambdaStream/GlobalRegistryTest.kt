package io.github.huherto.awsLambdaStream

import io.github.huherto.awsLambdaStream.faults.FaultManager
import io.github.huherto.awsLambdaStream.sinks.EventPublisher
import io.github.huherto.awsLambdaStream.sinks.EventPublisherInMemory
import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNotBe
import io.mockk.mockk
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

class GlobalRegistryTest {

    class TestEnvironmentConfig : EnvironmentConfig() {
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
    fun `envConfig factory should be lazy and return singleton`() {
        var createCount = 0

        GlobalRegistry.setEnvConfigFactory {
            createCount++
            object : EnvironmentConfig() {
                override fun awsLambdaFunctionName(): String? = "factory-function"
            }
        }

        createCount shouldBe 0

        val config1 = GlobalRegistry.envConfig()
        val config2 = GlobalRegistry.envConfig()

        config1 shouldBe config2
        createCount shouldBe 1
    }

    @Test
    fun `eventPublisher factory should be lazy and return singleton`() {
        var createCount = 0

        GlobalRegistry.setEventPublisherFactory {
            createCount++
            EventPublisherInMemory()
        }

        createCount shouldBe 0

        val pub1 = GlobalRegistry.eventPublisher()
        val pub2 = GlobalRegistry.eventPublisher()

        pub1 shouldBe pub2
        createCount shouldBe 1
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
    fun `reset should restore default factories`() {
        val customConfig = object : EnvironmentConfig() {
            override fun awsLambdaFunctionName(): String? = "custom-factory-function"
        }
        val customPublisher = EventPublisherInMemory()
        val customFaultManager = FaultManager(customPublisher)

        GlobalRegistry.setEnvConfigFactory { customConfig }
        GlobalRegistry.setEventPublisherFactory { customPublisher }
        GlobalRegistry.setFaultManagerFactory { customFaultManager }

        GlobalRegistry.envConfig() shouldBe customConfig
        GlobalRegistry.eventPublisher() shouldBe customPublisher
        GlobalRegistry.faultManager() shouldBe customFaultManager

        GlobalRegistry.reset()
        GlobalRegistry.setEnvConfig(TestEnvironmentConfig())

        GlobalRegistry.envConfig() shouldNotBe customConfig
        GlobalRegistry.eventPublisher() shouldNotBe customPublisher
        GlobalRegistry.faultManager() shouldNotBe customFaultManager
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

        fm.publisher() shouldBe customPub
    }
}
