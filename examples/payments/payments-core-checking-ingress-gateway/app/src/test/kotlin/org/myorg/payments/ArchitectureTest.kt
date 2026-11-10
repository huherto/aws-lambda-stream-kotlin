package org.myorg.payments

import io.kotest.matchers.collections.shouldBeEmpty
import org.junit.jupiter.api.Test

/**
 * An ingress gateway is an anti-corruption layer: it hears an external system and translates what it hears into our
 * events. It decides nothing, so it has no inside of its own: it is all adapter. See [Layering].
 */
class ArchitectureTest {

    @Test
    fun `the ingress gateway is all adapter - no domain or application layer to keep clean`() {
        Layering.layers().shouldBeEmpty()
    }
}
