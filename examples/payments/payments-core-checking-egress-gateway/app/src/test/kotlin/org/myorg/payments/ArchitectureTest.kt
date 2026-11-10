package org.myorg.payments

import io.kotest.matchers.collections.shouldBeEmpty
import io.kotest.matchers.collections.shouldContainExactly
import org.junit.jupiter.api.Test

/**
 * The inside of the hexagon doesn't depend on the outside: see [Layering].
 * A gateway has no business domain of its own; its inside is the policy for calling core checking.
 */
class ArchitectureTest {

    @Test
    fun `the gateway's inside is its application layer`() {
        Layering.layers() shouldContainExactly listOf("application")
    }

    @Test
    fun `calling once, the breaker and the reconciler depend on neither AWS nor the adapters`() {
        Layering.violations("application").shouldBeEmpty()
    }
}
