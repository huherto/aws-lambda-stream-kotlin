package io.github.huherto.awsLambdaStream

class EnvironmentConfig {

    fun tableName() : String? {
        var tableName = System.getenv("EVENT_TABLE_NAME")
        if (tableName == null || tableName.isEmpty()) {
            tableName = System.getenv("ENTITY_TABLE_NAME")
        }
        return tableName
    }

    fun awsRegion() : String {
        return System.getenv("AWS_REGION")
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
        return System.getenv("BUS_ENDPOINT_ID")
    }

}