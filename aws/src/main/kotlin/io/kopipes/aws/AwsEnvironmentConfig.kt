package io.kopipes.aws

import io.kopipes.core.EnvironmentConfig
import io.kopipes.core.GlobalRegistry

/** AWS-specific environment configuration. */
open class AwsEnvironmentConfig : EnvironmentConfig() {

    open fun tableName(): String? {
        return eventTableName() ?: entityTableName()
    }

    open fun eventTableName(): String? {
        return System.getenv("EVENT_TABLE_NAME")
    }

    open fun entityTableName(): String? {
        return System.getenv("ENTITY_TABLE_NAME")
    }

    override fun awsRegion(): String {
        return System.getenv("AWS_REGION") ?: System.getenv("AWS_DEFAULT_REGION") ?: "us-east-1"
    }

    override fun region(): String? {
        return System.getenv("AWS_REGION") ?: System.getenv("AWS_DEFAULT_REGION")
    }

    override fun accountName(): String? {
        return System.getenv("ACCOUNT_NAME")
    }

    override fun stage(): String? {
        return System.getenv("STAGE")
    }

    override fun serverlessStage(): String? {
        return System.getenv("SERVERLESS_STAGE")
    }

    override fun service(): String? {
        return System.getenv("SERVICE")
    }

    open fun awsDefaultRegion(): String? {
        return System.getenv("AWS_DEFAULT_REGION")
    }

    open fun endPointUrl(): String? {
        return System.getenv("AWS_ENDPOINT_URL")
    }

    open fun busName(): String? {
        return System.getenv("BUS_NAME")
    }

    open fun busSource(): String? {
        return System.getenv("BUS_SRC")
    }

    open fun busEndPointId(): String? {
        return System.getenv("BUS_ENDPOINT_ID")
    }

    open fun busTimeout(): Long? {
        return System.getenv("BUS_TIMEOUT")?.toLongOrNull()
    }

    open fun cloudWatchParallel(): Int? {
        return System.getenv("CW_PARALLEL")?.toIntOrNull()
    }

    open fun cloudWatchTimeout(): Long? {
        return System.getenv("CW_TIMEOUT")?.toLongOrNull()
    }

    open fun dynamodbTimeout(): Long? {
        return System.getenv("DYNAMODB_TIMEOUT")?.toLongOrNull()
    }

    override fun project(): String? {
        return System.getenv("PROJECT")
    }

    override fun serverlessProject(): String? {
        return System.getenv("SERVERLESS_PROJECT")
    }

    open fun bucketName(): String? {
        return System.getenv("BUCKET_NAME")
    }

    open fun nameSpace(): String? {
        return System.getenv("NAMESPACE")
    }

    companion object {
        val defaultInstance = AwsEnvironmentConfig()
    }
}

fun awsEnvConfig(): AwsEnvironmentConfig {
    return (GlobalRegistry.envConfig() as? AwsEnvironmentConfig) ?: AwsEnvironmentConfig.defaultInstance
}
