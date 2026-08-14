package io.github.huherto.awsLambdaStream

open class EnvironmentConfig {

    open fun tableName() : String? {
        return eventTableName() ?: entityTableName()
    }

    open fun eventTableName() : String? {
        return System.getenv("EVENT_TABLE_NAME")
    }

    open fun entityTableName() : String? {
        return System.getenv("ENTITY_TABLE_NAME")
    }

    open fun awsRegion() : String {
        return System.getenv("AWS_REGION")
    }

    open fun region(): String? {
        return System.getenv("AWS_REGION")
    }

    open fun accountName(): String? {
        return System.getenv("ACCOUNT_NAME")
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

    open fun awsDefaultRegion(): String? {
        return System.getenv("AWS_DEFAULT_REGION")
    }

    open fun endPointUrl() : String? {
        return System.getenv("AWS_ENDPOINT_URL")
    }

    open fun awsLambdaFunctionName(): String? {
        return System.getenv("AWS_LAMBDA_FUNCTION_NAME")
    }

    open fun ttl() : Int? {
        return System.getenv("TTL")?.toInt()
    }

    /**
     * If set to true, the stream will retry on failure. The pipeline will fail and will throw
     * an exception. The lambda handler will typically fail, which will cause kinesis to retry
     * the whole batch or bisect the batch.
     */
    open fun streamRetryEnabled() : Boolean {
        val enabled = System.getenv("STREAM_RETRY_ENABLED")
        return enabled != null && enabled.isNotEmpty() && enabled == "true"
    }

    /**
     * If set to true, the fault manager will save the retryable failures so they can be retried each. Use it along
     * with reportBatchItemFailures and the STREAM_RETRY_ENABLED flag.
     */
    open fun itemLevelRetryEnabled() : Boolean {
        val enabled = System.getenv("ITEM_LEVEL_RETRY_ENABLED")
        return enabled != null && enabled.isNotEmpty() && enabled == "true"
    }

    open fun busName() : String? {
        return System.getenv("BUS_NAME")
    }

    open fun busSource() : String? {
        return System.getenv("BUS_SRC")
    }

    open fun maxPublishRequestSize() : Int? {
        return System.getenv("PUBLISH_MAX_REQ_SIZE")?.toInt()
    }

    open fun maxRequestSize() : Int? {
        return System.getenv("MAX_REQ_SIZE")?.toInt()
    }

    open fun publishBatchSize() : Int? {
        return System.getenv("PUBLISH_BATCH_SIZE")?.toIntOrNull()
    }

    open fun batchSize() : Int? {
        return System.getenv("BATCH_SIZE")?.toIntOrNull()
    }

    open fun publishParallel() : Int? {
        return System.getenv("PUBLISH_PARALLEL")?.toIntOrNull()
    }

    open fun cloudWatchParallel() : Int? {
        return System.getenv("CW_PARALLEL")?.toIntOrNull()
    }

    open fun parallel() : Int? {
        return System.getenv("PARALLEL")?.toIntOrNull()
    }

    open fun busEndPointId() : String? {
        // Set this if you need to post to a global bus.
        return System.getenv("BUS_ENDPOINT_ID")
    }

    open fun busTimeout() : Long? {
        return System.getenv("BUS_TIMEOUT")?.toLongOrNull()
    }

    open fun cloudWatchTimeout() : Long? {
        return System.getenv("CW_TIMEOUT")?.toLongOrNull()
    }

    open fun timeout() : Long? {
        return System.getenv("TIMEOUT")?.toLongOrNull()
    }

    open fun project() : String? {
        return System.getenv("PROJECT")
    }

    open fun serverlessProject() : String? {
        return System.getenv("SERVERLESS_PROJECT")
    }

    open fun dynamodbTimeout() : Long? {
        return System.getenv("DYNAMODB_TIMEOUT")?.toLongOrNull()
    }

    open fun skip() : Boolean {
        return System.getenv("SKIP")?.toBoolean() ?: false
    }
    
    open fun unhealthy() : Boolean {
        return System.getenv("UNHEALTHY")?.toBoolean() ?: false
    }

    open fun bucketName() : String? {
        return System.getenv("BUCKET_NAME")
    }

    open fun serializationStrategy() : String? {
        return System.getenv("SERIALIZATION_STRATEGY")
    }

    open fun metrics(): String? {
        return System.getenv("METRICS")
    }

    open fun isMetricEnabled(key: String): Boolean {
        val config = metrics() ?: return false
        return config.contains(key) || config.contains("*")
    }

    open fun nameSpace(): String? {
        return System.getenv("NAMESPACE")
    }

//    open fun getProperty(name: String): String? {
//        return System.getenv(name)
//    }

}