package io.github.huherto.awsLambdaStream.sinks

import aws.sdk.kotlin.services.dynamodb.model.PutItemRequest
import aws.sdk.kotlin.services.dynamodb.model.PutItemResponse
import aws.sdk.kotlin.services.dynamodb.model.UpdateItemRequest
import aws.sdk.kotlin.services.dynamodb.model.UpdateItemResponse
import io.github.huherto.awsLambdaStream.EnvironmentConfig
import io.github.huherto.awsLambdaStream.FaultManager
import io.github.huherto.awsLambdaStream.UnitOfWork
import io.github.huherto.awsLambdaStream.connectors.DynamoDbConnector
import io.kotest.matchers.collections.shouldContainExactlyInAnyOrder
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.shouldBe
import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.confirmVerified
import io.mockk.mockk
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.async
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test
import kotlin.time.Duration.Companion.milliseconds

@OptIn(ExperimentalCoroutinesApi::class)
class DynamoDbSinkTest {

    private val envConfig = mockk<EnvironmentConfig>(relaxed = true)
    private val connector = mockk<DynamoDbConnector>()
    private val eventPublisher = mockk<EventPublisher>()
    private val fm = FaultManager(envConfig, eventPublisher, skipErrorLogging = true)

    @Test
    fun `update returns original unit of work when update request is missing`() = runTest {
        // arrange
        val sink = DynamoDbSink(envConfig, connector, parallel = 1)
        val uow = UnitOfWork(key = "without-update-request")

        // act
        val result = sink.update(fm, flowOf(uow)).toList()

        // assert
        result shouldBe listOf(uow)
        confirmVerified(connector)
    }

    @Test
    fun `put returns original unit of work when put request is missing`() = runTest {
        // arrange
        val sink = DynamoDbSink(envConfig, connector, parallel = 1)
        val uow = UnitOfWork(key = "without-put-request")

        // act
        val result = sink.put(fm, flowOf(uow)).toList()

        // assert
        result shouldBe listOf(uow)
        confirmVerified(connector)
    }

    @Test
    fun `update calls connector and stores update response`() = runTest {
        // arrange
        val sink = DynamoDbSink(envConfig, connector, parallel = 1)
        val request = UpdateItemRequest {}
        val response = UpdateItemResponse {}
        val uow = UnitOfWork(key = "update-item", updateRequest = request)

        coEvery { connector.update(request, uow) } returns response

        // act
        val result = sink.update(fm, flowOf(uow)).toList()

        // assert
        result shouldBe listOf(uow.copy(updateResponse = response))
        coVerify(exactly = 1) { connector.update(request, uow) }
        confirmVerified(connector)
    }

    @Test
    fun `put calls connector and stores put response`() = runTest {
        // arrange
        val sink = DynamoDbSink(envConfig, connector, parallel = 1)
        val request = PutItemRequest {}
        val response = PutItemResponse {}
        val uow = UnitOfWork(key = "put-item", putRequest = request)

        coEvery { connector.put(request, uow) } returns response

        // act
        val result = sink.put(fm, flowOf(uow)).toList()

        // assert
        result shouldBe listOf(uow.copy(putResponse = response))
        coVerify(exactly = 1) { connector.put(request, uow) }
        confirmVerified(connector)
    }

    @Test
    fun `update and put propagate connector failures through rejectWithFault`() = runTest {
        // arrange
        val sink = DynamoDbSink(envConfig, connector, parallel = 1)

        val updateRequest = UpdateItemRequest {}
        val updateUow = UnitOfWork(key = "failing-update", updateRequest = updateRequest)
        val updateError = IllegalStateException("update failed")

        val putRequest = PutItemRequest {}
        val putUow = UnitOfWork(key = "failing-put", putRequest = putRequest)
        val putError = IllegalArgumentException("put failed")

        coEvery { connector.update(updateRequest, updateUow) } throws updateError
        coEvery { connector.put(putRequest, putUow) } throws putError

        // act
        sink.update(fm, flowOf(updateUow)).toList()

        sink.put(fm, flowOf(putUow)).toList()

        // assert
        fm.getFaults() shouldHaveSize 2

        coVerify(exactly = 1) { connector.update(updateRequest, updateUow) }
        coVerify(exactly = 1) { connector.put(putRequest, putUow) }
        confirmVerified(connector)
    }

    @Test
    fun `mapParallel transforms every source value`() = runTest {
        // arrange
        val source = listOf(1, 2, 3).asFlow()

        // act
        val result = source
            .mapParallel(parallelism = 2) { value -> value * 10 }
            .toList()

        // assert
        result shouldContainExactlyInAnyOrder listOf(10, 20, 30)
    }

    @Test
    fun `mapParallel limits concurrent transforms to configured parallelism`() = runTest {
        // arrange
        val source = listOf(1, 2, 3).asFlow()
        val firstTwoTransformsStarted = CompletableDeferred<Unit>()
        var activeTransforms = 0
        var maxActiveTransforms = 0

        // act
        val collection = async {
            source
                .mapParallel(parallelism = 2) { value ->
                    activeTransforms += 1
                    maxActiveTransforms = maxOf(maxActiveTransforms, activeTransforms)

                    if (activeTransforms == 2) {
                        firstTwoTransformsStarted.complete(Unit)
                    }

                    delay(100.milliseconds)
                    activeTransforms -= 1

                    value
                }
                .toList()
        }

        firstTwoTransformsStarted.await()
        val result = collection.await()

        // assert
        result shouldHaveSize 3
        result shouldContainExactlyInAnyOrder listOf(1, 2, 3)
        maxActiveTransforms shouldBe 2
    }

}
