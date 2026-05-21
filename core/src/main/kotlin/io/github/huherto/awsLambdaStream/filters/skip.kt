package io.github.huherto.awsLambdaStream.filters

import io.github.huherto.awsLambdaStream.EnvironmentConfig
import io.github.huherto.awsLambdaStream.UnitOfWork

/**
 * Mark generated events with the skip tag.
 */
fun skipTag(envConfig: EnvironmentConfig): Map<String, Boolean?> {
    return mapOf(
        "skip" to envConfig.skip(),
    )
}

/**
 * Use this filter in pipelines to ignore test events.
 *
 * Returns true when the UnitOfWork should be kept.
 */
fun outSkip(uow: UnitOfWork): Boolean {
    return uow.event?.tags?.get("skip") != "true"
}