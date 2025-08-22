package org.myorg.sertrac

import software.amazon.awscdk.Stack
import software.constructs.Construct

enum class Stage(val id: String) {
    DEV("dev"),
    QA("qa"),
    PROD("prod");

    override fun toString(): String = id

    companion object {
        fun parse(value: String): org.myorg.sertrac.Stage =
            entries.firstOrNull { it.id.equals(value, ignoreCase = true) }
                ?: throw IllegalArgumentException("Unknown stage: $value")
    }
}

data class ServiceProps(val org : String, val subsys: String, val service: String, val stage: Stage, val region : String) {
}

open class BaseStack(scope: Construct, val serviceProps: ServiceProps) :
    Stack(scope, "${serviceProps.service}-${serviceProps.stage}") {

    public fun org() = serviceProps.org
    public fun subsys() = serviceProps.subsys
    public fun service() = serviceProps.service
    public fun stage() = serviceProps.stage
    public fun regionName() = serviceProps.region

}