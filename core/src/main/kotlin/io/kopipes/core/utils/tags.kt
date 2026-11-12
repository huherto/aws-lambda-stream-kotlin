package io.kopipes.core.utils

import io.kopipes.core.EnvironmentConfig
import io.kopipes.core.UnitOfWork

/** The tags every published event and fault carries, resolved once from configuration. */
data class StandardTags(
    val account: String = UNDEFINED,
    val region: String = UNDEFINED,
    val stage: String = UNDEFINED,
    val source: String = UNDEFINED,
    val functionName: String = UNDEFINED,
    val skip: Boolean = false,
) {
    constructor(envConfig: EnvironmentConfig) : this(
        account = envConfig.accountName() ?: UNDEFINED,
        region = envConfig.region() ?: UNDEFINED,
        stage = envConfig.stage() ?: envConfig.serverlessStage() ?: UNDEFINED,
        source = envConfig.service() ?: envConfig.project() ?: envConfig.serverlessProject() ?: UNDEFINED,
        functionName = envConfig.awsLambdaFunctionName() ?: UNDEFINED,
        skip = envConfig.skip(),
    )

    /** The environment tags for [pipeline], without the skip tag. */
    fun envTags(pipeline: String?): Map<String, String> = mapOf(
        "account" to account,
        "region" to region,
        "stage" to stage,
        "source" to source,
        "functionname" to functionName,
        "pipeline" to (pipeline ?: UNDEFINED),
    )

    private companion object {
        const val UNDEFINED = "undefined"
    }
}

/** Adds the standard tags to the event or fault in [uow]; tags already on the event win. */
fun adornStandardTags(uow: UnitOfWork, tags: StandardTags): UnitOfWork {
    val standard = tags.envTags(uow.pipeline?.id) + ("skip" to tags.skip.toString())
    val event = uow.event
    val fault = uow.fault

    if (event != null) {
        return uow.copy(event = event.copyEvent(tags = standard + event.tags.orEmpty()))
    } else if (fault != null) {
        return uow.copy(fault = fault.copy(tags = standard + fault.tags.orEmpty()))
    }
    return uow
}
