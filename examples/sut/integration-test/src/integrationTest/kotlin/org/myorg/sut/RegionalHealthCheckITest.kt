package org.myorg.sut

import io.kotest.matchers.nulls.shouldNotBeNull
import io.kotest.matchers.shouldBe
import kotlinx.coroutines.runBlocking
import mu.KotlinLogging
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.myorg.sut.facades.*

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class RegionalHealthCheckITest {

    private val logger = KotlinLogging.logger {}
    private val awsRegion = "us-east-1"
    private val tracerTableName = "sut-regional-health-check-local-tracer"
    private val bucketName = "myorg-sut-regional-health-check-local-us-east-1"
    private val streamName = "sut-regional-health-check-local-s1"
    private val checkHealthApiFacade = CheckHealthApiFacade()
    private val s3Facade = S3Facade()
    private val dynamoDbFacade = DynamoDbFacade(tracerTableName)
    private val kinesisFacade = KinesisFacade(streamName = streamName)

    @Test
    fun verifyCheckHealthTriggersTraceActions(): Unit = runBlocking {
        // Arrange

        // Act
        val response = checkHealthApiFacade.check()

        // Assert
        verifyTracerReachesDynamoDbTable(response)

        verifyTracerReachesS3(response)

        verifyTracerReachesKinesisStream(response)

    }

    private suspend fun verifyTracerReachesDynamoDbTable(response: HealthCheckResponse) {
        // Assert
        val savedRecord = dynamoDbFacade.findEntityByPK(awsRegion) { items ->
            items?.firstOrNull { item ->
                item["sk"]?.asS() == response.timestamp.toString()
            }
        }

        savedRecord.shouldNotBeNull()
        savedRecord["pk"]?.asS() shouldBe awsRegion
        savedRecord["sk"]?.asS() shouldBe response.timestamp.toString()
        savedRecord["status"]?.asS() shouldBe "STARTED"
        savedRecord["discriminator"]?.asS() shouldBe "trace"
        savedRecord["awsregion"]?.asS() shouldBe awsRegion
        savedRecord["timestamp"]?.asN().shouldNotBeNull()
        savedRecord["ttl"]?.asN().shouldNotBeNull()
    }


    private suspend fun verifyTracerReachesS3(response: HealthCheckResponse) {
        val expectedKey = "$awsRegion/${response.timestamp}"

        val content = s3Facade.getObjectWithKey(bucketName, expectedKey)

        content.shouldNotBeNull()
        content.contains(awsRegion) shouldBe true
        content.contains(response.timestamp.toString()) shouldBe true
        content.contains("STARTED") shouldBe true
    }

    private suspend fun verifyTracerReachesKinesisStream(response: HealthCheckResponse) {
        val matchingRecord: String? = kinesisFacade.waitForResult(
            onTimeout = {
                logger.error { "Timed out waiting for kinesis record." }
                null
            },
            block = {
                val records = kinesisFacade.readAllEvents()
                records
                    .onEach { record -> logger.info { "Kinesis record: $record" } }
                    .firstOrNull { record ->
                    record.contains(awsRegion) &&
                            record.contains(response.timestamp.toString()) &&
                            record.contains("STARTED")
                }
            },
        )

        matchingRecord.shouldNotBeNull()
        matchingRecord.contains(awsRegion) shouldBe true
        matchingRecord.contains(response.timestamp.toString()) shouldBe true
        matchingRecord.contains("STARTED") shouldBe true
    }

    @AfterAll
    fun tearDownAll() {
        s3Facade.close()
        dynamoDbFacade.close()
        kinesisFacade.close()
    }
}