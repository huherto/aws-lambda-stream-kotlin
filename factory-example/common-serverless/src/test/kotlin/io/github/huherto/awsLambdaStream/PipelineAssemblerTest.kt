package io.github.huherto.awsLambdaStream

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.mapNotNull
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

class PipelineAssemblerTest {

    // A mock pipeline to test how the assembler routes flows
    class MockPipeline(id: String) : Pipeline(id) {
        override fun connect(fm: FaultManager, fromFlow: Flow<UnitOfWork>): Flow<UnitOfWork> {
            return fromFlow.map { it.copy(key = "processed_by_$id") }
        }
    }

    // A mock pipeline that intentionally throws a FailureException to test error handling
    class FailingPipeline(id: String) : Pipeline(id) {
        override fun connect(fm: FaultManager, fromFlow: Flow<UnitOfWork>): Flow<UnitOfWork> {
            with(fm) {
                return fromFlow.mapNotNull {
                    faulty(it) {
                        throw FailureException(it, RuntimeException("Intentional failure for $id"))
                    }
                }
            }
        }
    }

    @Test
    fun `test builder creates assembler with added pipelines`() {
        val pipeline1 = MockPipeline("p1")
        val pipeline2 = MockPipeline("p2")

        val assembler = PipelineAssembler.builder()
            .addPipeline(pipeline1)
            .addPipeline(pipeline2)
            .build()

        assertNotNull(assembler)
    }

    @Test
    fun `test assemble routes UnitOfWork through all pipelines`() = runBlocking {
        val pipeline1 = MockPipeline("p1")
        val pipeline2 = MockPipeline("p2")

        val assembler = PipelineAssembler.builder()
            .addPipeline(pipeline1)
            .addPipeline(pipeline2)
            .build()

        // Provide a mock flow
        val headFlow = flowOf(UnitOfWork(record = "test_record"))

        // Disable fault handler to focus on routing and processing 
        val resultFlow = assembler.assemble(headFlow, includeFaultHandler = false)
        val results = resultFlow.toList()

        // It should duplicate the UoW into every pipeline, resulting in 2 outputs
        assertEquals(2, results.size, "Should produce two outputs, one from each pipeline")

        val keys = results.map { it.key }.toSet()
        assertTrue(keys.contains("processed_by_p1"), "Pipeline 1 should have processed an item")
        assertTrue(keys.contains("processed_by_p2"), "Pipeline 2 should have processed an item")

        // Assert that the `pipeline` field was populated correctly on `onEach`
        val p1Result = results.find { it.key == "processed_by_p1" }
        assertEquals(pipeline1, p1Result?.pipeline)

        val p2Result = results.find { it.key == "processed_by_p2" }
        assertEquals(pipeline2, p2Result?.pipeline)
    }

    @Test
    fun `test assemble with includeFaultHandler set to true`() = runBlocking {
        val pipeline1 = MockPipeline("p1")

        val assembler = PipelineAssembler.builder()
            .addPipeline(pipeline1)
            .build()

        val headFlow = flowOf(UnitOfWork(record = "test_record"))

        // Implicitly checks that catchFailures doesn't break standard flow processing
        val resultFlow = assembler.assemble(headFlow, includeFaultHandler = true)
        val results = resultFlow.toList()

        assertEquals(1, results.size)
        assertEquals("processed_by_p1", results.first().key)
    }

    @Test
    fun `test assemble handles FailureException and publishes FailureEvent`() = runBlocking {
        val failingPipeline = FailingPipeline("fail1")
        val fm = FaultManager()

        val assembler = PipelineAssembler.builder()
            .addPipeline(failingPipeline)
            .faultManager(fm)
            .build()

        val uow = UnitOfWork(record = "test_record")
        val headFlow = flowOf(uow)

        // When the flow is assembled, it will be mapped into the FailingPipeline and throw an exception
        val resultFlow = assembler.assemble(headFlow, includeFaultHandler = true)

        val results = resultFlow.toList()
        // The resulting flow itself should be empty because the exception was thrown before it could emit
        assertEquals(0, results.size, "Flow should be empty due to failure")

        // Verify that flushFaults() successfully moved the caught exception from `theFaults` queue into the `published` queue
        assertEquals(1, fm.getPublished().size, "Should publish one failure event")
        assertEquals(0, fm.getFaults().size, "Faults should be empty after publishing")

        val failureEvent = fm.getPublished().get(0)
        assertNotNull(failureEvent)
        assertEquals(FAULT_EVENT_TYPE, failureEvent.eventType())
        
        // Assert that the proper exception type is saved within the FailureEvent
        val exception = failureEvent.failureException
        assertNotNull(exception)
        
        // Check that the UnitOfWork matches the failing UoW
        assertEquals(uow.record, exception?.uom?.record)
    }

    @Test
    fun `test startPipeline and endPipeline simply return uow`() {
        val assembler = PipelineAssembler.builder().build()
        val uow = UnitOfWork(key = "test")

        val startedUow = assembler.startPipeline(uow)
        assertEquals(uow, startedUow)

        val endedUow = assembler.endPipeline(uow)
        assertEquals(uow, endedUow)
    }
}