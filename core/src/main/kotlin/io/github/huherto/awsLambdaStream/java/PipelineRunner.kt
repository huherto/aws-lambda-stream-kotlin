package io.github.huherto.awsLambdaStream.java

import io.github.huherto.awsLambdaStream.PipelineAssembler
import io.github.huherto.awsLambdaStream.UnitOfWork
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.runBlocking
import java.util.function.Consumer
import kotlin.coroutines.EmptyCoroutineContext

typealias  TransformerFunction = (Flow<UnitOfWork>) -> Flow<UnitOfWork>

sealed class Step{
    data class Transformer(val f: TransformerFunction) : Step()
    data class OnEach(val f: Consumer<UnitOfWork>) : Step()
}

class PipelineRunner<I> (
     private val assembler: PipelineAssembler,
) {

    private var headFlowCreator : ((I) -> Flow<UnitOfWork>)? = null

    private val steps = mutableListOf<Step>()

    fun headFlow(hfc : (I) -> Flow<UnitOfWork>) : PipelineRunner<I> {
        headFlowCreator = hfc;
        return this
    }

    fun transformer(f: TransformerFunction) : PipelineRunner<I> {
        steps.add(Step.Transformer(f))
        return this
    }

    fun onEach(f: Consumer<UnitOfWork>) : PipelineRunner<I> {
        steps.add(Step.OnEach(f))
        return this
    }

    fun run(input: I) {
        require(headFlowCreator != null) { "headFlowCreator is required" }
        try {
            runBlocking(EmptyCoroutineContext) {
                val headFlow = headFlowCreator!!(input)
                val assembler = assembler

                var flow = assembler.assemble(headFlow)
                for(step in steps) {
                    when(step ) {
                        is Step.Transformer -> {
                            flow = step.f(flow)
                        }
                        is Step.OnEach -> {
                            flow = flow.onEach { uow ->
                                step.f.accept(uow)
                            }
                        }
                    }
                }

                flow.collect()
            }
        } catch (e: InterruptedException) {
            Thread.currentThread().interrupt()
            throw RuntimeException(e)
        }
    }
}