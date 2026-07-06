package org.myorg.sut

import io.kotest.matchers.nulls.shouldNotBeNull
import io.kotest.matchers.shouldBe
import kotlinx.coroutines.runBlocking
import mu.KotlinLogging
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.myorg.sut.facades.CheckHealthApiFacade
import org.myorg.sut.facades.DynamoDbFacade
import org.myorg.sut.facades.HealthCheckResponse
import org.myorg.sut.facades.S3Facade

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class RegionalHealthCheckITest {

    private val logger = KotlinLogging.logger {}
    private val awsRegion = "us-east-1"
    private val tracerTableName = "sut-regional-health-check-local-tracer"
    private val bucketName = "myorg-sut-regional-health-check-local-us-east-1"
    private val checkHealthApiFacade = CheckHealthApiFacade()
    private val s3Facade = S3Facade()
    private val dynamoDbFacade = DynamoDbFacade(tracerTableName)

    @Test
    fun verifyCheckHealthTriggersTraceActions(): Unit = runBlocking {
        // Arrange

        // Act
        val response = checkHealthApiFacade.check()

        // Assert
        verifyTracerReachesDynamoDbTable(response)

        verifyTracerReachesS3(response)

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

    @AfterAll
    fun tearDownAll() {
        s3Facade.close()
        dynamoDbFacade.close()
    }
}