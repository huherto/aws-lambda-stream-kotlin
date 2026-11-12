package io.kopipes.spring.lambda

import io.kopipes.core.JsonEventCodec
import io.kopipes.core.PipelineAssembler
import io.kopipes.core.UnitOfWork
import io.kopipes.core.faults.FaultManager
import io.kopipes.core.flavors.CollectPipeline
import io.kopipes.core.sinks.EventPublisherInMemory
import io.kopipes.core.sinks.EventsMicrostoreInMemory
import io.kotest.matchers.collections.shouldContainExactly
import kotlinx.coroutines.flow.asFlow
import org.junit.jupiter.api.Test

class PipelineConsumersTest {

    @Test
    fun `pipelineConsumer runs every accepted event through the assembler`() {
        // arrange
        val microstore = EventsMicrostoreInMemory()
        val assembler = PipelineAssembler.builder()
            .faultManager(FaultManager(EventPublisherInMemory(), skipErrorLogging = true))
            .addPipeline(CollectPipeline.builder().id("collect").eventsMicrostore(microstore).build())
            .build()
        val consumer = pipelineConsumer<List<String>>(assembler) { ids ->
            ids.map { UnitOfWork(event = JsonEventCodec.decode("""{"id":"$it","type":"T","timestamp":1}""")) }.asFlow()
        }

        // act
        consumer.accept(listOf("a", "b"))
        consumer.accept(listOf("c"))

        // assert
        microstore.getStored().keys shouldContainExactly setOf("a", "b", "c")
    }
}
