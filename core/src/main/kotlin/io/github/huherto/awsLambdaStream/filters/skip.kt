package io.github.huherto.awsLambdaStream.filters

import io.github.huherto.awsLambdaStream.GlobalRegistry.envConfig
import io.github.huherto.awsLambdaStream.UnitOfWork

fun skipTag(): Map<String, Boolean?> {
    return mapOf(
        "skip" to envConfig().skip(),
    )
}

fun outSkip(uow: UnitOfWork): Boolean {
    return uow.event?.tags?.get("skip") != "true"
}