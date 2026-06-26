package org.myorg.sut

import aws.sdk.kotlin.services.dynamodb.model.AttributeValue
import aws.sdk.kotlin.services.dynamodb.model.UpdateItemResponse
import io.github.huherto.awsLambdaStream.sinks.DynamoDbUpdateValue
import io.kotest.matchers.collections.shouldBeEmpty
import io.kotest.matchers.doubles.shouldBeLessThan
import io.kotest.matchers.longs.shouldBeLessThan
import io.kotest.matchers.maps.shouldContainKey
import io.kotest.matchers.shouldBe
import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.mockk
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test
import kotlin.math.abs

class ModelTest {

    private val awsRegion = "us-east-1"

    @Test
    fun `check returns service unavailable without calling connector when unhealthy flag is true`() = runTest {
        // Arrange
        val connector = mockk<Connector>(relaxed = true)
        val model = Model(
            connector = connector,
            unhealthyFlag = true,
            awsRegion = awsRegion,
        )

        // Act
        val response = model.check()

        // Assert
        response.statusCode shouldBe 503
        response.region shouldBe awsRegion
        response.incomplete shouldBe null
        response.elapsed shouldBe null
        response.get shouldBe null
        response.save shouldBe null

        coVerify(exactly = 0) { connector.get(any()) }
        coVerify(exactly = 0) { connector.update(any(), any()) }
    }

    @Test
    fun `check returns ok when most recent trace is completed and not older than one minute`() {

        runTest {
            // Arrange
            val connector = mockk<Connector>()
            val saveResponse = mockk<UpdateItemResponse>()
            val mostRecentSk = roundToNearestMinute(now()) + 60_000L
            val getResponse = listOf(
                mapOf(
                    "status" to AttributeValue.S("COMPLETED"),
                    "sk" to AttributeValue.S(mostRecentSk.toString()),
                ),
            )

            coEvery { connector.get(awsRegion) } returns getResponse
            coEvery { connector.update(any(), any()) } returns saveResponse

            val model = Model(connector = connector, awsRegion = awsRegion)

            // Act
            val response = model.check()

            // Assert
            response.statusCode shouldBe 200
            response.region shouldBe awsRegion
            response.incomplete shouldBe false
            response.get shouldBe getResponse
            response.save shouldBe saveResponse
            response.elapsed!! shouldBeLessThan 1.0

            coVerify(exactly = 1) { connector.get(awsRegion) }
            coVerify(exactly = 1) { connector.update(any(), any()) }
        }
    }

    @Test
    fun `check returns service unavailable when most recent trace is incomplete`() = runTest {
        // Arrange
        val connector = mockk<Connector>()
        val saveResponse = mockk<UpdateItemResponse>()
        val mostRecentSk = roundToNearestMinute(now()) + 60_000L
        val getResponse = listOf(
            mapOf(
                "status" to AttributeValue.S("STARTED"),
                "sk" to AttributeValue.S(mostRecentSk.toString()),
            ),
        )

        coEvery { connector.get(awsRegion) } returns getResponse
        coEvery { connector.update(any(), any()) } returns saveResponse

        val model = Model(connector = connector, awsRegion = awsRegion)

        // Act
        val response = model.check()

        // Assert
        response.statusCode shouldBe 503
        response.region shouldBe awsRegion
        response.incomplete shouldBe true
        response.get shouldBe getResponse
        response.save shouldBe saveResponse

        coVerify(exactly = 1) { connector.get(awsRegion) }
        coVerify(exactly = 1) { connector.update(any(), any()) }
    }

    @Test
    fun `check returns service unavailable when most recent trace is older than one minute`() = runTest {
        // Arrange
        val connector = mockk<Connector>()
        val saveResponse = mockk<UpdateItemResponse>()
        val oldSk = roundToNearestMinute(now()) - 120_000L
        val getResponse = listOf(
            mapOf(
                "status" to AttributeValue.S("COMPLETED"),
                "sk" to AttributeValue.S(oldSk.toString()),
            ),
        )

        coEvery { connector.get(awsRegion) } returns getResponse
        coEvery { connector.update(any(), any()) } returns saveResponse

        val model = Model(connector = connector, awsRegion = awsRegion)

        // Act
        val response = model.check()

        // Assert
        response.statusCode shouldBe 503
        response.region shouldBe awsRegion
        response.incomplete shouldBe false
        response.get shouldBe getResponse
        response.save shouldBe saveResponse
        response.elapsed!! shouldBe 2.0

        coVerify(exactly = 1) { connector.get(awsRegion) }
        coVerify(exactly = 1) { connector.update(any(), any()) }
    }

    @Test
    fun `check treats missing most recent trace as unavailable`() = runTest {
        // Arrange
        val connector = mockk<Connector>()
        val saveResponse = mockk<UpdateItemResponse>()

        coEvery { connector.get(awsRegion) } returns emptyList()
        coEvery { connector.update(any(), any()) } returns saveResponse

        val model = Model(connector = connector, awsRegion = awsRegion)

        // Act
        val response = model.check()

        // Assert
        response.statusCode shouldBe 503
        response.region shouldBe awsRegion
        response.incomplete shouldBe false
        response.get.shouldBeEmpty()
        response.save shouldBe saveResponse

        coVerify(exactly = 1) { connector.get(awsRegion) }
        coVerify(exactly = 1) { connector.update(any(), any()) }
    }

    @Test
    fun `get delegates to connector with aws region`() = runTest {
        // Arrange
        val connector = mockk<Connector>()
        val getResponse = listOf(
            mapOf(
                "status" to AttributeValue.S("COMPLETED"),
                "sk" to AttributeValue.S("123"),
            ),
        )

        coEvery { connector.get(awsRegion) } returns getResponse

        val model = Model(connector = connector, awsRegion = awsRegion)

        // Act
        val response = model.get()

        // Assert
        response shouldBe getResponse

        coVerify(exactly = 1) { connector.get(awsRegion) }
    }

    @Test
    fun `save writes started trace for current aws region`() = runTest {
        // Arrange
        val connector = mockk<Connector>()
        val saveResponse = mockk<UpdateItemResponse>()
        val capturedKey = mutableListOf<Map<String, AttributeValue>>()
        val capturedInputParams = mutableListOf<Map<String, DynamoDbUpdateValue>>()

        coEvery {
            connector.update(capture(capturedKey), capture(capturedInputParams))
        } returns saveResponse

        val model = Model(connector = connector, awsRegion = awsRegion)

        // Act
        val response = model.save()

        // Assert
        response shouldBe saveResponse

        val key = capturedKey.single()
        key["pk"] shouldBe AttributeValue.S(awsRegion)
        abs(key["sk"]!!.asS().toLong() - roundToNearestMinute(now())) shouldBeLessThan 60_001

        val inputParams = capturedInputParams.single()
        inputParams shouldContainKey "timestamp"
        inputParams["status"] shouldBe DynamoDbUpdateValue.DbSet(AttributeValue.S("STARTED"))
        inputParams["discriminator"] shouldBe DynamoDbUpdateValue.DbSet(AttributeValue.S(DISCRIMINATOR))
        inputParams["latched"] shouldBe DynamoDbUpdateValue.DbRemove
        inputParams shouldContainKey "ttl"
        inputParams["awsregion"] shouldBe DynamoDbUpdateValue.DbSet(AttributeValue.S(awsRegion))

        coVerify(exactly = 1) { connector.update(any(), any()) }
    }
}