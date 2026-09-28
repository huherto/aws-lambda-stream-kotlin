package io.kopipes.aws.utils

import io.kopipes.aws.GlobalRegistry.envConfig
import io.kopipes.aws.UnitOfWork
import io.kopipes.aws.filters.skipTag

fun adornStandardTags(
    uow: UnitOfWork,
): UnitOfWork {
    val event = uow.event
    val fault = uow.fault

    if (event != null) {
        return uow.copy(
            event = event.copyEvent(
                tags = envTags(uow.pipeline?.id) +
                        skipTag().mapValues { it.value.toString() } +
                        event.tags.orEmpty()
            )
        )
    } else if (fault != null) {
        return uow.copy(
            fault = fault.copy(
                tags = envTags( uow.pipeline?.id) +
                        skipTag().mapValues { it.value.toString() } +
                        fault.tags.orEmpty()
            )
        )
    }

    return uow
}

fun envTags(
    pipeline: String?,
): Map<String, String> {
    return mapOf(
        "account" to (envConfig().accountName() ?: "undefined"),
        "region" to (envConfig().region() ?: "undefined"),
        "stage" to (envConfig().stage() ?: envConfig().serverlessStage() ?: "undefined"),
        "source" to (
            envConfig().service()
                ?: envConfig().project()
                ?: envConfig().serverlessProject()
                ?: "undefined"
            ),
        "functionname" to (envConfig().awsLambdaFunctionName() ?: "undefined"),
        "pipeline" to (pipeline ?: "undefined"),
    )
}
