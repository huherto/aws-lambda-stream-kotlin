package io.kopipes.aws.filters

import io.kopipes.aws.GlobalRegistry.envConfig
import io.kopipes.aws.UnitOfWork

fun skipTag(): Map<String, Boolean?> {
    return mapOf(
        "skip" to envConfig().skip(),
    )
}

fun outSkip(uow: UnitOfWork): Boolean {
    return uow.event?.tags?.get("skip") != "true"
}