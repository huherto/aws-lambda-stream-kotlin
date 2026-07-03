package org.myorg.sut

import aws.sdk.kotlin.services.s3.listObjectsV2
import aws.sdk.kotlin.services.s3.model.GetObjectRequest
import aws.smithy.kotlin.runtime.content.decodeToString
import io.kotest.matchers.nulls.shouldNotBeNull
import io.kotest.matchers.shouldBe
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.myorg.sut.facades.AwsFacade
import kotlin.time.Duration.Companion.milliseconds

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class RegionalHealthCheckITest {

    private val awsRegion = "us-east-1"
    private val tracerTableName = "sut-regional-health-check-local-tracer"
    private val checkHealthApiFacade = CheckHealthApiFacade()
    private val awsFacade = AwsFacade(entityTable = tracerTableName)

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
        val savedRecord = awsFacade.findEntityByPK(awsRegion) { items ->
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
        val bucketName = "myorg-sut-regional-health-check-local-us-east-1"
        val expectedKey = "$awsRegion/${response.timestamp}"
        val startTime = System.currentTimeMillis()

        while (true) {
            val listResponse = awsFacade.s3Client.listObjectsV2 {
                bucket = bucketName
                prefix = expectedKey
            }

            val foundKey = listResponse.contents
                .orEmpty()
                .mapNotNull { it.key }
                .firstOrNull { it == expectedKey }

            if (foundKey != null) {
                val content = awsFacade.s3Client.getObject(GetObjectRequest {
                    bucket = bucketName
                    key = foundKey
                }) { s3Response ->
                    s3Response.body?.decodeToString()
                }

                content.shouldNotBeNull()
                content.contains(awsRegion) shouldBe true
                content.contains(response.timestamp.toString()) shouldBe true
                content.contains("STARTED") shouldBe true

                return
            }

            if (System.currentTimeMillis() - startTime > 20000) {
                error("Timed out waiting for tracer S3 object: s3://$bucketName/$expectedKey")
            }

            delay(1000.milliseconds)
        }
    }

    @AfterAll
    fun tearDownAll() {
        awsFacade.closeAll()
    }
}