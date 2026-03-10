package io.github.huherto.awsLambdaStream

import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class FiltersTest {

    @Test
    fun `test event types filter`() = runTest {

        val faultManager = FaultManager()

        val  myEventA = MyEventA()
        val  myEventB = MyEventB()
        val  myEventC = MyEventC()

        var foundMyEventA = false
        var foundMyEventB = false
        var foundMyEventC = false

        flow{
            emit(UnitOfWork(event = myEventA ))
            emit(UnitOfWork(event = myEventB ))
            emit(UnitOfWork(event = myEventC ))
        }.filterEventTypes(faultManager, MyEventA::class, MyEventB::class)
            .onEach {
                when (it.event) {
                    is MyEventA -> foundMyEventA = true
                    is MyEventB -> foundMyEventB = true
                    is MyEventC -> foundMyEventC = true
                    null -> TODO()
                }
            }.collect {  }
        assertTrue { foundMyEventA }
        assertTrue { foundMyEventB }
        assertFalse { foundMyEventC }
    }

}