package io.github.huherto.awsLambdaStream.utils

import io.github.huherto.awsLambdaStream.EnvironmentConfig
import io.github.huherto.awsLambdaStream.UnitOfWork
import io.github.huherto.awsLambdaStream.filters.skipTag

fun adornStandardTags(
    envConfig: EnvironmentConfig,
    uow: UnitOfWork,
): UnitOfWork {
    val event = uow.event
    val fault = uow.fault

    if (event != null) {
        return uow.copy(
            event = event.copyEvent(
                tags = envTags(envConfig, uow.pipeline?.id) +
                        skipTag(envConfig).mapValues { it.value.toString() } +
                        event.tags.orEmpty()
            )
        )
    } else if (fault != null) {
        return uow.copy(
            fault = fault.copy(
                tags = envTags(envConfig, uow.pipeline?.id) +
                        skipTag(envConfig).mapValues { it.value.toString() } +
                        fault.tags.orEmpty()
            )
        )
    }

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
