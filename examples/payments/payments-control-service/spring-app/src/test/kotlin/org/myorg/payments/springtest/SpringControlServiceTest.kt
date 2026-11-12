package org.myorg.payments.springtest

import io.kopipes.aws.AwsKopipesContext
import io.kopipes.aws.testsupport.TestContext
import io.kopipes.core.sinks.EventPublisherInMemory
import io.kopipes.core.sinks.EventsMicrostoreInMemory
import io.kopipes.spring.lambda.SpringDynamoDbHandler
import io.kopipes.spring.lambda.SpringKinesisHandler
import io.kopipes.spring.lambda.SpringLambdaContexts
import io.kotest.matchers.collections.shouldContainExactlyInAnyOrder
import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.shouldBeInstanceOf
import io.kotest.matchers.types.shouldBeSameInstanceAs
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.myorg.payments.Fixtures
import org.myorg.payments.PaymentsEvent
import org.myorg.payments.WithdrawalRequested
import org.myorg.payments.adapters.inbound.ListenerContainer
import org.myorg.payments.adapters.inbound.StreamRecords
import org.myorg.payments.adapters.inbound.StreamRecords.correlationItem
import org.myorg.payments.adapters.inbound.StreamRecords.insert
import org.myorg.payments.adapters.inbound.TriggerContainer
import org.myorg.payments.spring.ControlServiceConfiguration
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Import
import org.springframework.context.annotation.Primary

/** The real configuration, with the bus and the events table replaced by in-memory ones. */
@SpringBootApplication(proxyBeanMethods = false)
@Import(ControlServiceConfiguration::class)
class TestControlServiceApplication {
    @Bean
    fun inMemoryPublisher(): EventPublisherInMemory = EventPublisherInMemory()

    @Bean
    @Primary
    fun inMemoryEventsMicrostore(): EventsMicrostoreInMemory = EventsMicrostoreInMemory()
}

class TestListener : SpringKinesisHandler(TestControlServiceApplication::class.java)

class TestTrigger : SpringDynamoDbHandler(TestControlServiceApplication::class.java)

class SpringControlServiceTest {

    @AfterEach
    fun tearDown() {
        SpringLambdaContexts.closeAll()
    }

    private fun <T : Any> bean(type: Class<T>): T = SpringLambdaContexts.get(TestControlServiceApplication::class.java).getBean(type)

    @Test
    fun `the listener collects payment events by event id and skips the others`() {
        // arrange
        val listener = TestListener()
        val kinesisEvent = StreamRecords.kinesisEvent(Fixtures.paymentSubmitted(), Fixtures.fundsWithdrawn(), Fixtures.accountChanged())

        // act
        listener.handleRequest(kinesisEvent, TestContext())

        // assert
        bean(EventsMicrostoreInMemory::class.java).getStored().keys shouldContainExactlyInAnyOrder listOf("p-7.submitted", "p-7.withdrawn")
    }

    @Test
    fun `the trigger runs the payment rules and publishes on the bus`() {
        // arrange
        val trigger = TestTrigger()
        val submitted = Fixtures.paymentSubmitted()

        // act
        trigger.handleRequest(StreamRecords.dynamodbEvent(insert(correlationItem(submitted))), TestContext())

        // assert
        val published = bean(EventPublisherInMemory::class.java).uows().mapNotNull { it.event as? PaymentsEvent }
        val requested = published.single().shouldBeInstanceOf<WithdrawalRequested>()
        requested.id shouldBe "p-7.start"
    }

    @Test
    fun `the containers are wired from the kopipes context bean`() {
        // arrange
        TestListener()
        val kopipes = bean(AwsKopipesContext::class.java)

        // act
        val listener = bean(ListenerContainer::class.java)
        val trigger = bean(TriggerContainer::class.java)

        // assert
        listener.context shouldBeSameInstanceAs kopipes
        trigger.context shouldBeSameInstanceAs kopipes
        trigger.eventPublisher shouldBeSameInstanceAs bean(EventPublisherInMemory::class.java)
        listener.assembler.getFaultManager() shouldBeSameInstanceAs kopipes.faultManager
    }
}
