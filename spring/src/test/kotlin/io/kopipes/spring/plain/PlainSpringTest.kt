package io.kopipes.spring.plain

import com.amazonaws.services.lambda.runtime.events.KinesisEvent
import io.kopipes.aws.AwsEnvironmentConfig
import io.kopipes.aws.AwsKopipesContext
import io.kopipes.aws.from.KinesisAdapter
import io.kopipes.aws.sinks.EventBridgePublisher
import io.kopipes.aws.testsupport.TestContext
import io.kopipes.core.JsonEventCodec
import io.kopipes.core.PipelineAssembler
import io.kopipes.core.flavors.CollectPipeline
import io.kopipes.core.sinks.EventsMicrostoreInMemory
import io.kopipes.spring.KopipesConfiguration
import io.kopipes.spring.SpringEnvironmentConfig
import io.kopipes.spring.lambda.SpringKinesisHandler
import io.kopipes.spring.lambda.SpringLambdaContexts
import io.kopipes.spring.lambda.kinesisConsumer
import io.kotest.matchers.collections.shouldContainExactly
import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.shouldBeInstanceOf
import io.kotest.matchers.types.shouldBeSameInstanceAs
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.springframework.context.annotation.AnnotationConfigApplicationContext
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Import
import org.springframework.core.env.MapPropertySource
import java.nio.ByteBuffer
import java.util.function.Consumer

/** A Spring Framework application without Spring Boot. */
@Configuration(proxyBeanMethods = false)
@Import(KopipesConfiguration::class)
class PlainStreamApplication {
    @Bean
    fun microstore(): EventsMicrostoreInMemory = EventsMicrostoreInMemory()

    @Bean
    fun listener(kopipes: AwsKopipesContext, microstore: EventsMicrostoreInMemory): Consumer<KinesisEvent> {
        val assembler = PipelineAssembler.builder()
            .context(kopipes)
            .addPipeline(CollectPipeline.builder().id("collect").eventsMicrostore(microstore).build())
            .build()
        return kinesisConsumer(assembler, KinesisAdapter(kopipes, JsonEventCodec))
    }
}

class PlainListener : SpringKinesisHandler(PlainStreamApplication::class.java)

class PlainSpringTest {

    @AfterEach
    fun tearDown() {
        SpringLambdaContexts.closeAll()
    }

    @Test
    fun `KopipesConfiguration gives a plain Spring context the kopipes context, read from the Environment`() {
        // arrange
        val spring = AnnotationConfigApplicationContext()
        spring.environment.propertySources.addFirst(MapPropertySource("test", mapOf("kopipes.bus-name" to "plain-bus")))
        spring.register(KopipesConfiguration::class.java)

        // act
        spring.refresh()
        val kopipes = spring.getBean(AwsKopipesContext::class.java)

        // assert
        kopipes.envConfig shouldBeSameInstanceAs spring.getBean(AwsEnvironmentConfig::class.java)
        kopipes.envConfig.shouldBeInstanceOf<SpringEnvironmentConfig>()
        kopipes.eventPublisher.shouldBeInstanceOf<EventBridgePublisher>().busName shouldBe "plain-bus"
        kopipes.faultManager.publisher() shouldBeSameInstanceAs kopipes.eventPublisher
        spring.close()
    }

    @Test
    fun `a handler starts a plain configuration without Spring Boot and runs its consumer`() {
        // arrange
        val handler = PlainListener()
        val event = KinesisEvent().apply {
            records = listOf(KinesisEvent.KinesisEventRecord().apply {
                eventID = "shard:e-1"
                kinesis = KinesisEvent.Record().apply {
                    sequenceNumber = "1"
                    data = ByteBuffer.wrap("""{"id":"e-1","type":"T","timestamp":1}""".toByteArray())
                }
            })
        }

        // act
        handler.handleRequest(event, TestContext())

        // assert
        val spring = SpringLambdaContexts.get(PlainStreamApplication::class.java)
        spring.shouldBeInstanceOf<AnnotationConfigApplicationContext>()
        spring.containsBean("springApplicationArguments") shouldBe false
        spring.getBean(EventsMicrostoreInMemory::class.java).getStored().keys shouldContainExactly setOf("e-1")
    }

    @Test
    fun `Spring Boot applications are told apart from plain ones by their annotation`() {
        // arrange & act & assert
        SpringLambdaContexts.isSpringBootApplication(PlainStreamApplication::class.java) shouldBe false
        SpringLambdaContexts.isSpringBootApplication(io.kopipes.spring.lambda.app.TestStreamApplication::class.java) shouldBe true
    }
}
