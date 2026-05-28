package io.github.huherto.awsLambdaStream.utils

import io.github.huherto.awsLambdaStream.EnvironmentConfig
import io.github.huherto.awsLambdaStream.UnitOfWork
import io.github.huherto.awsLambdaStream.filters.skipTag

fun adornStandardTags(
    envConfig: EnvironmentConfig,
    uow: UnitOfWork,
): UnitOfWork {
    val event = uow.event ?: return uow

    event.tags = envTags(envConfig, uow.pipeline?.id) +
        skipTag(envConfig).mapValues { it.value.toString() } +
        event.tags.orEmpty()

    return uow
}

fun envTags(
    envConfig: EnvironmentConfig,
    pipeline: String?,
): Map<String, String> {
    return mapOf(
        "account" to (envConfig.accountName() ?: "undefined"),
        "region" to (envConfig.region() ?: "undefined"),
        "stage" to (envConfig.stage() ?: envConfig.serverlessStage() ?: "undefined"),
        "source" to (
            envConfig.service()
                ?: envConfig.project()
                ?: envConfig.serverlessProject()
                ?: "undefined"
            ),
        "functionname" to (envConfig.awsLambdaFunctionName() ?: "undefined"),
        "pipeline" to (pipeline ?: "undefined"),
    )
}
