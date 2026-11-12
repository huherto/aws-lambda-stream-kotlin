package io.kopipes.spring

import io.kopipes.aws.AwsEnvironmentConfig
import org.springframework.core.env.Environment

/**
 * [AwsEnvironmentConfig] backed by the Spring [Environment] instead of `System.getenv`.
 *
 * Each setting is looked up first as a `kopipes.*` property (e.g. `kopipes.bus-name`, which Spring's relaxed binding
 * also fills from the `KOPIPES_BUS_NAME` env var), then under its legacy env var name (e.g. `BUS_NAME`). The Spring
 * Environment includes the OS environment, so a Lambda deployed with the usual env vars behaves exactly as without Spring,
 * and the same keys can also be set in `application.yml`, profiles, or test properties.
 */
open class SpringEnvironmentConfig(private val env: Environment) : AwsEnvironmentConfig() {

    protected fun property(key: String, envVar: String): String? =
        env.getProperty("$PREFIX.$key") ?: env.getProperty(envVar)

    private fun int(key: String, envVar: String): Int? = property(key, envVar)?.toIntOrNull()

    private fun long(key: String, envVar: String): Long? = property(key, envVar)?.toLongOrNull()

    // ---- EnvironmentConfig ----

    override fun ttl(): Int? = int("ttl", "TTL")

    override fun streamRetryEnabled(): Boolean = property("stream-retry-enabled", "STREAM_RETRY_ENABLED") == "true"

    override fun itemLevelRetryEnabled(): Boolean = property("item-level-retry-enabled", "ITEM_LEVEL_RETRY_ENABLED") == "true"

    override fun maxPublishRequestSize(): Int? = int("publish-max-req-size", "PUBLISH_MAX_REQ_SIZE")

    override fun maxRequestSize(): Int? = int("max-req-size", "MAX_REQ_SIZE")

    override fun publishBatchSize(): Int? = int("publish-batch-size", "PUBLISH_BATCH_SIZE")

    override fun batchSize(): Int? = int("batch-size", "BATCH_SIZE")

    override fun publishParallel(): Int? = int("publish-parallel", "PUBLISH_PARALLEL")

    override fun parallel(): Int? = int("parallel", "PARALLEL")

    override fun timeout(): Long? = long("timeout", "TIMEOUT")

    override fun skip(): Boolean = property("skip", "SKIP")?.toBoolean() ?: false

    override fun unhealthy(): Boolean = property("unhealthy", "UNHEALTHY")?.toBoolean() ?: false

    override fun awsLambdaFunctionName(): String? = property("aws-lambda-function-name", "AWS_LAMBDA_FUNCTION_NAME")

    override fun accountName(): String? = property("account-name", "ACCOUNT_NAME")

    override fun region(): String? = property("region", "AWS_REGION") ?: awsDefaultRegion()

    override fun awsRegion(): String = region() ?: "us-east-1"

    override fun stage(): String? = property("stage", "STAGE")

    override fun serverlessStage(): String? = property("serverless-stage", "SERVERLESS_STAGE")

    override fun service(): String? = property("service", "SERVICE")

    override fun project(): String? = property("project", "PROJECT")

    override fun serverlessProject(): String? = property("serverless-project", "SERVERLESS_PROJECT")

    override fun metrics(): String? = property("metrics", "METRICS")

    // ---- AwsEnvironmentConfig ----

    override fun eventTableName(): String? = property("event-table-name", "EVENT_TABLE_NAME")

    override fun entityTableName(): String? = property("entity-table-name", "ENTITY_TABLE_NAME")

    override fun awsDefaultRegion(): String? = property("aws-default-region", "AWS_DEFAULT_REGION")

    override fun endPointUrl(): String? = property("endpoint-url", "AWS_ENDPOINT_URL")

    override fun busName(): String? = property("bus-name", "BUS_NAME")

    override fun busSource(): String? = property("bus-source", "BUS_SRC")

    override fun busEndPointId(): String? = property("bus-endpoint-id", "BUS_ENDPOINT_ID")

    override fun busTimeout(): Long? = long("bus-timeout", "BUS_TIMEOUT")

    override fun cloudWatchParallel(): Int? = int("cloud-watch-parallel", "CW_PARALLEL")

    override fun cloudWatchTimeout(): Long? = long("cloud-watch-timeout", "CW_TIMEOUT")

    override fun dynamodbTimeout(): Long? = long("dynamodb-timeout", "DYNAMODB_TIMEOUT")

    override fun bucketName(): String? = property("bucket-name", "BUCKET_NAME")

    override fun nameSpace(): String? = property("namespace", "NAMESPACE")

    companion object {
        const val PREFIX = "kopipes"
    }
}
