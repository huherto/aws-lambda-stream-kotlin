package io.kopipes.core

/** Configuration backed by environment variables for stream operations. */
open class EnvironmentConfig {

    open fun ttl(): Int? {
        return System.getenv("TTL")?.toIntOrNull()
    }

    open fun streamRetryEnabled(): Boolean {
        val enabled = System.getenv("STREAM_RETRY_ENABLED")
        return enabled != null && enabled.isNotEmpty() && enabled == "true"
    }

    open fun itemLevelRetryEnabled(): Boolean {
        val enabled = System.getenv("ITEM_LEVEL_RETRY_ENABLED")
        return enabled != null && enabled.isNotEmpty() && enabled == "true"
    }

    open fun maxPublishRequestSize(): Int? {
        return System.getenv("PUBLISH_MAX_REQ_SIZE")?.toIntOrNull()
    }

    open fun maxRequestSize(): Int? {
        return System.getenv("MAX_REQ_SIZE")?.toIntOrNull()
    }

    open fun publishBatchSize(): Int? {
        return System.getenv("PUBLISH_BATCH_SIZE")?.toIntOrNull()
    }

    open fun batchSize(): Int? {
        return System.getenv("BATCH_SIZE")?.toIntOrNull()
    }

    open fun publishParallel(): Int? {
        return System.getenv("PUBLISH_PARALLEL")?.toIntOrNull()
    }

    open fun parallel(): Int? {
        return System.getenv("PARALLEL")?.toIntOrNull()
    }

    open fun timeout(): Long? {
        return System.getenv("TIMEOUT")?.toLongOrNull()
    }

    open fun skip(): Boolean {
        return System.getenv("SKIP")?.toBoolean() ?: false
    }

    open fun unhealthy(): Boolean {
        return System.getenv("UNHEALTHY")?.toBoolean() ?: false
    }

    open fun awsLambdaFunctionName(): String? {
        return System.getenv("AWS_LAMBDA_FUNCTION_NAME")
    }

    open fun accountName(): String? {
        return System.getenv("ACCOUNT_NAME")
    }

    open fun region(): String? {
        return System.getenv("AWS_REGION") ?: System.getenv("AWS_DEFAULT_REGION")
    }

    open fun awsRegion(): String {
        return region() ?: "us-east-1"
    }

    open fun stage(): String? {
        return System.getenv("STAGE")
    }

    open fun serverlessStage(): String? {
        return System.getenv("SERVERLESS_STAGE")
    }

    open fun service(): String? {
        return System.getenv("SERVICE")
    }

    open fun project(): String? {
        return System.getenv("PROJECT")
    }

    open fun serverlessProject(): String? {
        return System.getenv("SERVERLESS_PROJECT")
    }

    open fun metrics(): String? {
        return System.getenv("METRICS")
    }

    open fun isMetricEnabled(key: String): Boolean {
        val config = metrics() ?: return false
        return config.contains(key) || config.contains("*")
    }
}
