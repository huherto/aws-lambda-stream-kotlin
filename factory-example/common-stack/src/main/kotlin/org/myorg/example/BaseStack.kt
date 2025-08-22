package org.myorg.example

import software.amazon.awscdk.Stack
import software.constructs.Construct

open class BaseStack(scope: Construct, id: String) : Stack(scope, id) {

    private fun org() = "myorg"
    private fun subsys() = "template"

}