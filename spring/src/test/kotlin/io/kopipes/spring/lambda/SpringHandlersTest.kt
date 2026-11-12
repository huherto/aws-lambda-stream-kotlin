package io.kopipes.spring.lambda

import com.amazonaws.services.lambda.runtime.events.DynamodbEvent
import com.amazonaws.services.lambda.runtime.events.KinesisEvent
import io.kopipes.aws.testsupport.TestContext
import io.kopipes.core.sinks.EventsMicrostoreInMemory
import io.kopipes.spring.lambda.app.AuditLog
import io.kopipes.spring.lambda.app.TestAudit
import io.kopipes.spring.lambda.app.TestListener
import io.kopipes.spring.lambda.app.TestStreamApplication
import io.kopipes.spring.lambda.app.TestTrigger
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.collections.shouldContainExactly
import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.shouldBeSameInstanceAs
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.NoUniqueBeanDefinitionException
import java.nio.ByteBuffer

class SpringHandlersTest {

    @AfterEach
    fun tearDown() {
        SpringLambdaContexts.closeAll()
    }

    private fun kinesisEvent(vararg ids: String): KinesisEvent = KinesisEvent().apply {
        records = ids.map { id ->
            KinesisEvent.KinesisEventRecord().apply {
                eventID = "shard:$id"
                kinesis = KinesisEvent.Record().apply {
                    sequenceNumber = id
                    data = ByteBuffer.wrap("""{"id":"$id","type":"THING_HAPPENED","timestamp":1,"partitionKey":"pk-$id"}""".toByteArray())
                }
            }
        }
    }

    private fun bean(type: Class<*>) = SpringLambdaContexts.get(TestStreamApplication::class.java).getBean(type)

    @Test
    fun `a kinesis handler runs each event through the application's pipelines`() {
        // arrange
        val handler = TestListener()

        // act
        handler.handleRequest(kinesisEvent("e-1", "e-2"), TestContext())

        // assert
        val microstore = bean(EventsMicrostoreInMemory::class.java) as EventsMicrostoreInMemory
        microstore.getStored().keys shouldContainExactly setOf("e-1", "e-2")
    }

    @Test
    fun `handlers of one application share a single context`() {
        // arrange
        TestListener()
        val first = SpringLambdaContexts.get(TestStreamApplication::class.java)

        // act
        TestTrigger().handleRequest(DynamodbEvent().apply { records = emptyList() }, TestContext())

        // assert
        SpringLambdaContexts.get(TestStreamApplication::class.java) shouldBeSameInstanceAs first
    }

    @Test
    fun `a bean name picks one of several consumers of the same event type`() {
        // arrange
        val audit = TestAudit()

        // act
        audit.handleRequest(kinesisEvent("e-1", "e-2", "e-3"), TestContext())

        // assert
        (bean(AuditLog::class.java) as AuditLog).records shouldBe 3
        (bean(EventsMicrostoreInMemory::class.java) as EventsMicrostoreInMemory).getStored().size shouldBe 0
    }

    @Test
    fun `without a name, several consumers of the same type is an error`() {
        // arrange & act & assert
        shouldThrow<NoUniqueBeanDefinitionException> {
            SpringLambdaContexts.consumer(TestStreamApplication::class.java, KinesisEvent::class.java)
        }
    }
}
