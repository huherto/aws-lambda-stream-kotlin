package io.github.huherto.`aws-lambda-stream`

class EnvironmentConfig {

    fun tableName() : String {
        var tableName = System.getenv("EVENT_TABLE_NAME")
        if (tableName == null || tableName.isEmpty()) {
            tableName = System.getenv("ENTITY_TABLE_NAME")
        }
        return tableName;
    }

    fun awsRegion() : String {
        return System.getenv("AWS_REGION")
    }
}