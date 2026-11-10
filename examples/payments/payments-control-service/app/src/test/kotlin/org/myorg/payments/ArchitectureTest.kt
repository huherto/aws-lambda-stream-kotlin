package org.myorg.payments

import io.kotest.matchers.collections.shouldBeEmpty
import io.kotest.matchers.collections.shouldContain
import org.junit.jupiter.api.Test

/** The inside of the hexagon doesn't depend on the outside: see [Layering]. */
class ArchitectureTest {

    @Test
    fun `the control service has a domain`() {
        Layering.layers() shouldContain "domain"
    }

    @Test
    fun `the domain depends on neither kopipes nor AWS nor the adapters`() {
        Layering.violations("domain").shouldBeEmpty()
    }

    @Test
    fun `the application layer depends on neither AWS nor the adapters`() {
        Layering.violations("application").shouldBeEmpty()
    }
}
