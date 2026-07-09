package org.myorg.sut

import aws.sdk.kotlin.services.dynamodb.model.AttributeValue
import aws.sdk.kotlin.services.dynamodb.model.UpdateItemResponse
import io.github.huherto.awsLambdaStream.sinks.DynamoDbUpdateValue
import io.github.huherto.awsLambdaStream.utils.ttl
import io.kotest.matchers.collections.shouldContainExactly
import io.kotest.matchers.maps.shouldContainExactly
import io.kotest.matchers.shouldBe
import io.mockk.*
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test
import kotlin.time.Duration.Companion.days

class TracerDaoTest {

    @Test
    fun `getTracers maps DynamoDB items to tracers`() = runTest {
        // Arrange
        val awsRegion = "eu-west-1"
        val connector = mockk<Connector>()

        coEvery {
            connector.get(awsRegion)
        } returns listOf(
            mapOf(
                "sk" to AttributeValue.S("1710000000000"),
                "timestamp" to AttributeValue.N("1710000000123"),
                "ttl" to AttributeValue.N("1710005520"),
                "status" to AttributeValue.S("COMPLETED"),
            ),
            mapOf(
                "sk" to AttributeValue.S("1710000060000"),
                "timestamp" to AttributeValue.N("1710000060123"),
                "ttl" to AttributeValue.N("1710005580"),
                "status" to AttributeValue.S("STARTED"),
            ),
        )

        val dao = TracerDao(
            connector = connector,
            awsRegion = awsRegion,
        )

        // Act
        val result = dao.getTracers()

        // Assert
        result.shouldContainExactly(
            Tracer(
                awsRegion = awsRegion,
                roundedTimestamp = 1710000000000,
                timestamp = 1710000000123,
                ttl = 1710005520,
                status = "COMPLETED",
            ),
            Tracer(
                awsRegion = awsRegion,
                roundedTimestamp = 1710000060000,
                timestamp = 1710000060123,
                ttl = 1710005580,
                status = "STARTED",
            ),
        )

        coVerify(exactly = 1) {
            connector.get(awsRegion)
        }
    }

    @Test
    fun `getTracers uses defaults when DynamoDB values are missing or invalid`() = runTest {
        // Arrange
        val awsRegion = "eu-west-1"
        val connector = mockk<Connector>()

        coEvery {
            connector.get(awsRegion)
        } returns listOf(
            mapOf(
                "sk" to AttributeValue.S("not-a-number"),
                "timestamp" to AttributeValue.Bool(true),
            ),
        )

        val dao = TracerDao(
            connector = connector,
            awsRegion = awsRegion,
        )

        // Act
        val result = dao.getTracers()

        // Assert
        result.shouldContainExactly(
            Tracer(
                awsRegion = awsRegion,
                roundedTimestamp = 0L,
                timestamp = 0L,
                ttl = 0L,
                status = "UNKNOWN",
            ),
        )

        coVerify(exactly = 1) {
            connector.get(awsRegion)
        }
    }

    @Test
    fun `save updates tracer attributes using region and rounded timestamp as key`() = runTest {
        // Arrange
        val awsRegion = "eu-west-1"
        val connector = mockk<Connector>()
        val keySlot = slot<Map<String, AttributeValue>>()
        val inputParamsSlot = slot<Map<String, DynamoDbUpdateValue>>()
        val response = UpdateItemResponse {}

        coEvery {
            connector.update(
                key = capture(keySlot),
                inputParams = capture(inputParamsSlot),
            )
        } returns response

        val dao = TracerDao(
            connector = connector,
            awsRegion = awsRegion,
        )

        val tracer = Tracer(
            awsRegion = awsRegion,
            roundedTimestamp = 1710000000000,
            timestamp = 1710000000123,
            ttl = 1710005520,
            status = "STARTED",
        )

        // Act
        val result = dao.save(tracer)

        // Assert
        result shouldBe response

        keySlot.captured.shouldContainExactly(
            mapOf(
                "pk" to AttributeValue.S(awsRegion),
                "sk" to AttributeValue.S("1710000000000"),
            ),
        )

        inputParamsSlot.captured.shouldContainExactly(
            mapOf(
                "timestamp" to DynamoDbUpdateValue.DbSet(AttributeValue.N("1710000000123")),
                "status" to DynamoDbUpdateValue.DbSet(AttributeValue.S("STARTED")),
                "discriminator" to DynamoDbUpdateValue.DbSet(AttributeValue.S(DISCRIMINATOR)),
                "latched" to DynamoDbUpdateValue.DbRemove,
                "ttl" to DynamoDbUpdateValue.DbSet(AttributeValue.N("1710005520")),
                "awsregion" to DynamoDbUpdateValue.DbSet(AttributeValue.S(awsRegion)),
            ),
        )

        coVerify(exactly = 1) {
            connector.update(any(), any())
        }
    }

    @Test
    fun `check returns unhealthy response without reading or saving when unhealthy flag is true`() = runTest {
        // Arrange
        val awsRegion = "eu-west-1"
        val connector = mockk<Connector>()
        val dao = TracerDao(
            connector = connector,
            awsRegion = awsRegion,
        )

        // Act
        val result = dao.check(unhealthyFlag = true)

        // Assert
        result.statusCode shouldBe 503
        result.region shouldBe awsRegion
        result.incomplete shouldBe null
        result.elapsed shouldBe null
        result.tracers shouldBe null
        result.saveResponse shouldBe null

        confirmVerified(connector)
    }

    @Test
    fun `check returns healthy when most recent tracer is completed and recent`() = runTest {
        // Arrange
        val awsRegion = "eu-west-1"
        val connector = mockk<Connector>()
        val saveResponse = UpdateItemResponse {}

        val recentRoundedTimestamp = truncateToMinute(System.currentTimeMillis())

        coEvery {
            connector.get(awsRegion)
        } returns listOf(
            mapOf(
                "sk" to AttributeValue.S(recentRoundedTimestamp.toString()),
                "timestamp" to AttributeValue.N(recentRoundedTimestamp.toString()),
                "ttl" to AttributeValue.N(ttl(recentRoundedTimestamp, 92.days).toString()),
                "status" to AttributeValue.S("COMPLETED"),
            ),
        )

        coEvery {
            connector.update(any(), any())
        } returns saveResponse

        val dao = TracerDao(
            connector = connector,
            awsRegion = awsRegion,
        )

        // Act
        val result = dao.check()

        // Assert
        result.statusCode shouldBe 200
        result.region shouldBe awsRegion
        result.incomplete shouldBe false
        result.elapsed shouldBe 0.0
        result.tracers.shouldContainExactly(
            Tracer(
                awsRegion = awsRegion,
                roundedTimestamp = recentRoundedTimestamp,
                timestamp = recentRoundedTimestamp,
                ttl = ttl(recentRoundedTimestamp, 92.days),
                status = "COMPLETED",
            ),
        )
        result.saveResponse shouldBe saveResponse.toString()

        coVerify(exactly = 1) {
            connector.get(awsRegion)
            connector.update(any(), any())
        }
    }

    @Test
    fun `check returns unhealthy when most recent tracer is incomplete`() = runTest {
        // Arrange
        val awsRegion = "eu-west-1"
        val connector = mockk<Connector>()
        val saveResponse = UpdateItemResponse {}

        val recentRoundedTimestamp = truncateToMinute(System.currentTimeMillis())

        coEvery {
            connector.get(awsRegion)
        } returns listOf(
            mapOf(
                "sk" to AttributeValue.S(recentRoundedTimestamp.toString()),
                "timestamp" to AttributeValue.N(recentRoundedTimestamp.toString()),
                "ttl" to AttributeValue.N(ttl(recentRoundedTimestamp, 92.days).toString()),
                "status" to AttributeValue.S("STARTED"),
            ),
        )

        coEvery {
            connector.update(any(), any())
        } returns saveResponse

        val dao = TracerDao(
            connector = connector,
            awsRegion = awsRegion,
        )

        // Act
        val result = dao.check()

        // Assert
        result.statusCode shouldBe 503
        result.region shouldBe awsRegion
        result.incomplete shouldBe true
        result.elapsed shouldBe 0.0
        result.saveResponse shouldBe saveResponse.toString()

        coVerify(exactly = 1) {
            connector.get(awsRegion)
            connector.update(any(), any())
        }
    }
}