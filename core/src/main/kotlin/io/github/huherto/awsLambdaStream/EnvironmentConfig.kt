package io.github.huherto.awsLambdaStream

class EnvironmentConfig {

    fun tableName() : String? {
        return eventTableName() ?: entityTableName()
    }

    fun eventTableName() : String? {
        return System.getenv("EVENT_TABLE_NAME")
    }

    fun entityTableName() : String? {
        return System.getenv("ENTITY_TABLE_NAME")
    }

    fun awsRegion() : String {
        return System.getenv("AWS_REGION")
    }

    fun region(): String? {
        return System.getenv("AWS_REGION")
    }

    fun accountName(): String? {
        return System.getenv("ACCOUNT_NAME")
    }

    fun stage(): String? {
        return System.getenv("STAGE")
    }

    fun serverlessStage(): String? {
        return System.getenv("SERVERLESS_STAGE")
    }

    fun service(): String? {
        return System.getenv("SERVICE")
    }

    fun awsDefaultRegion(): String? {
        return System.getenv("AWS_DEFAULT_REGION")
    }

    fun endPointUrl() : String? {
        return System.getenv("AWS_ENDPOINT_URL")
    }

    fun awsLambdaFunctionName(): String? {
        return System.getenv("AWS_LAMBDA_FUNCTION_NAME")
    }

    fun ttl() : Int? {
        return System.getenv("TTL")?.toInt()
    }

    fun streamRetryEnabled() : Boolean {
        val enabled = System.getenv("STREAM_RETRY_ENABLED")
        return enabled != null && enabled.isNotEmpty() && enabled == "true"
    }

    fun busName() : String? {
        return System.getenv("BUS_NAME")
    }

    fun busSource() : String? {
        return System.getenv("BUS_SRC")
    }

    fun maxPublishRequestSize() : Int? {
        return System.getenv("PUBLISH_MAX_REQ_SIZE")?.toInt()
    }

    fun maxRequestSize() : Int? {
        return System.getenv("MAX_REQ_SIZE")?.toInt()
    }

    fun publishBatchSize() : Int? {
        return System.getenv("PUBLISH_BATCH_SIZE")?.toIntOrNull()
    }

    fun batchSize() : Int? {
        return System.getenv("BATCH_SIZE")?.toIntOrNull()
    }

    fun publishParallel() : Int? {
        return System.getenv("PUBLISH_PARALLEL")?.toIntOrNull()
    }

    fun parallel() : Int? {
        return System.getenv("PARALLEL")?.toIntOrNull()
    }

    fun busEndPointId() : String? {
        // Set this if you need to post to a global bus.
        return System.getenv("BUS_ENDPOINT_ID")
    }

    fun busTimeout() : Long? {
        return System.getenv("BUS_TIMEOUT")?.toLongOrNull()
    }

    fun timeout() : Long? {
        return System.getenv("TIMEOUT")?.toLongOrNull()
    }

    fun project() : String? {
        return System.getenv("PROJECT")
    }

    fun serverlessProject() : String? {
        return System.getenv("SERVERLESS_PROJECT")
    }

    fun dynamodbTimeout() : Long? {
        return System.getenv("DYNAMODB_TIMEOUT")?.toLongOrNull()
    }

    fun skip() : Boolean {
        return System.getenv("SKIP")?.toBoolean() ?: false
    }
    
    fun unhealthy() : Boolean {
        return System.getenv("UNHEALTHY")?.toBoolean() ?: false
    }

}