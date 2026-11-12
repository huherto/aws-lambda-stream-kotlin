package org.myorg.payments

import io.kotest.matchers.collections.shouldBeEmpty
import io.kotest.matchers.collections.shouldContainExactlyInAnyOrder
import org.junit.jupiter.api.Test

/** The inside of the hexagon doesn't depend on the outside: see [Layering]. */
class ArchitectureTest {

    @Test
    fun `the BFF has a domain and an application layer`() {
        Layering.layers() shouldContainExactlyInAnyOrder listOf("domain", "application")
    }

    @Test
    fun `the domain depends on neither kopipes nor AWS nor the other layers`() {
        Layering.violations("domain").shouldBeEmpty()
    }

    @Test
    fun `the application layer depends on neither AWS nor the adapters`() {
        Layering.violations("application").shouldBeEmpty()
    }
}
