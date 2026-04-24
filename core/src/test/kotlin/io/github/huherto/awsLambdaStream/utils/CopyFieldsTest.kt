package io.github.huherto.awsLambdaStream.utils

import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.shouldBe
import kotlin.test.Test

class CopyFieldsTest {

    data class Source(
        var id: String? = "source-id",
        var shared: String? = "source-shared",
        var nullable: String? = null,
        val readOnly: String = "read-only-source",
        var extra: String? = "extra-value",
    )

    data class Target(
        var id: String? = "target-id",
        var shared: String? = "target-shared",
        var nullable: String? = "target-nullable",
        val readOnly: String = "read-only-target",
    )

    data class ConstructorTarget(
        var id: String,
        var shared: String,
        var nullable: String?
    )

    class NoPrimaryConstructorTarget private constructor() {
        var id: String? = null

        companion object {
            fun create() = NoPrimaryConstructorTarget()
        }
    }

    @Test
    fun `should copy matching mutable properties from source to target`() {
        val from = Source(
            id = "new-id",
            shared = "new-shared",
            nullable = "nullable-value",
            extra = "ignored-extra"
        )
        val to = Target()

        copyCommonFields(from, to)

        to.id shouldBe "new-id"
        to.shared shouldBe "new-shared"
        to.nullable shouldBe "nullable-value"
        to.readOnly shouldBe "read-only-target"
    }

    @Test
    fun `should not overwrite target values when source property is null`() {
        val from = Source(
            id = null,
            shared = "copied-shared",
            nullable = null
        )
        val to = Target(
            id = "existing-id",
            shared = "existing-shared",
            nullable = "existing-nullable"
        )

        copyCommonFields(from, to)

        to.id shouldBe "existing-id"
        to.shared shouldBe "copied-shared"
        to.nullable shouldBe "existing-nullable"
    }

    @Test
    fun `should ignore properties that do not exist on the target`() {
        val from = Source(extra = "still-ignored")
        val to = Target()

        copyCommonFields(from, to)

        to.id shouldBe "source-id"
        to.shared shouldBe "source-shared"
        to.nullable shouldBe "target-nullable"
    }

    @Test
    fun `should create an instance from matching constructor parameters`() {
        val from = Source(
            id = "copied-id",
            shared = "copied-shared",
            nullable = null
        )

        val result = createFromCommonValues(from, ConstructorTarget::class)

        result.id shouldBe "copied-id"
        result.shared shouldBe "copied-shared"
        result.nullable shouldBe null
    }

    @Test
    fun `should use the factory when one is provided`() {
        val from = Source(
            id = "factory-id",
            shared = "factory-shared",
            nullable = "factory-nullable"
        )

        val result = createFromCommonValues(from, ConstructorTarget::class) {
            ConstructorTarget(
                id = "created-by-factory",
                shared = "created-by-factory",
                nullable = "created-by-factory"
            )
        }

        result.id shouldBe "factory-id"
        result.shared shouldBe "factory-shared"
        result.nullable shouldBe "factory-nullable"
    }

    @Test
    fun `should throw when target class has no primary constructor and no factory is provided`() {
        val from = Source()

        shouldThrow<IllegalArgumentException> {
            createFromCommonValues(from, NoPrimaryConstructorTarget::class)
        }
    }
}
