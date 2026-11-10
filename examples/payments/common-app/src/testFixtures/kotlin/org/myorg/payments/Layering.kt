package org.myorg.payments

import java.io.File

/**
 * The hexagon's dependency rule, checked on a service's sources: the inside never depends on the outside.
 *
 * - `domain` (business rules) may use the event model (`org.myorg.payments`) and the standard library only:
 *   no kopipes, no AWS, no adapters, no application layer.
 * - `application` (use cases and the ports they need) may use the domain, but no AWS and no adapters.
 *   It may use kopipes' core types where a port is kopipes' own (e.g. `EventPublisher`), never its AWS module.
 *
 * Adapters (`adapters.inbound`, `adapters.outbound`) may depend on anything: they are where the outside lives.
 *
 * The event model itself implements kopipes' `Event` (its envelope: id, timestamp, partition key, raw): the event is
 * the domain's language and the framework's message at once. That's a deliberate trade-off of this example.
 */
object Layering {

    private val forbidden = mapOf(
        "domain" to listOf(
            "io.kopipes.", "aws.", "com.amazonaws.", "software.amazon.",
            "org.myorg.payments.adapters.", "org.myorg.payments.application.",
        ),
        "application" to listOf(
            "io.kopipes.aws.", "aws.", "com.amazonaws.", "software.amazon.",
            "org.myorg.payments.adapters.",
        ),
    )

    /** Every import in [layer] that breaks the rule, as `file: import`. Run from the module directory. */
    fun violations(layer: String, sources: File = File("src/main/kotlin/org/myorg/payments")): List<String> {
        val prefixes = forbidden[layer] ?: error("No rule for layer $layer")
        val dir = File(sources, layer)
        if (!dir.isDirectory) return emptyList()
        return dir.walkTopDown().filter { it.isFile && it.extension == "kt" }.flatMap { file ->
            file.readLines()
                .map { it.trim() }
                .filter { it.startsWith("import ") }
                .map { it.removePrefix("import ").trim() }
                .filter { imported -> prefixes.any { imported.startsWith(it) } }
                .map { "${file.relativeTo(sources)}: $it" }
        }.toList()
    }

    /** The layers this service has, out of domain and application. */
    fun layers(sources: File = File("src/main/kotlin/org/myorg/payments")): List<String> =
        forbidden.keys.filter { File(sources, it).isDirectory }
}
