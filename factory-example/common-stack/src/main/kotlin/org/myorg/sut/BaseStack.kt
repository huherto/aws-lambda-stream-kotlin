package org.myorg.sut

import software.amazon.awscdk.Stack
import software.constructs.Construct

enum class Stage(val id: String) {
    LOCAL("local"),
    DEV("dev"),
    QA("qa"),
    PROD("prod");

    override fun toString(): String = id

    companion object {
        fun parse(value: String): org.myorg.sut.Stage =
            entries.firstOrNull { it.id.equals(value, ignoreCase = true) }
                ?: throw IllegalArgumentException("Unknown stage: $value")
    }
}

data class ServiceProps(val org : String, val subsys: String, val service: String, val stage: Stage, val region : String)

open class BaseStack(scope: Construct, val serviceProps: ServiceProps) :
    Stack(scope, "${serviceProps.service}-${serviceProps.stage}") {

    fun org() = serviceProps.org
    fun subsys() = serviceProps.subsys
    fun service() = serviceProps.service
    fun stage() = serviceProps.stage
    fun regionName() = serviceProps.region

}