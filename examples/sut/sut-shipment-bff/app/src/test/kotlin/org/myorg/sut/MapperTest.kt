package org.myorg.sut

import io.kotest.matchers.shouldBe
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test


class MapperTest {
    @Test
    fun `sortKeyTransform returns the value after the first delimiter`() {
        // Arrange
        val sortKey = "order|123"

        // Act
        val result = sortKeyTransform(sortKey)

        // Assert
        result shouldBe "123"
    }

    @Test
    fun `deletedFilter returns true when item is not marked as deleted`() {
        // Arrange
        val item = mutableMapOf<String, Any?>(
            "id" to "item-1",
            "deleted" to false,
        )

        // Act
        val result = deletedFilter(item)

        // Assert
        result shouldBe true
    }

    @Test
    fun `deletedFilter returns false when item is marked as deleted`() {
        // Arrange
        val item = mutableMapOf<String, Any?>(
            "id" to "item-1",
            "deleted" to true,
        )

        // Act
        val result = deletedFilter(item)

        // Assert
        result shouldBe false
    }

    @Test
    fun `mapper applies default rename and omit rules`(): Unit = runBlocking {
        // Arrange
        val item = mutableMapOf<String, Any?>(
            "pk" to "customer-1",
            "sk" to "customer|profile",
            "name" to "Ada",
            "deleted" to false,
        )
        val ctx = emptyMap<String, Any?>()
        val mapItem = mapper()

        // Act
        val result = mapItem(item, ctx)

        // Assert
        result shouldBe mapOf(
            "id" to "customer-1",
            "name" to "Ada",
        )
    }

    @Test
    fun `mapper applies defaults before item values so item values can override defaults`(): Unit = runBlocking {
        // Arrange
        val item = mutableMapOf<String, Any?>(
            "name" to "Ada",
            "status" to "active",
        )
        val ctx = emptyMap<String, Any?>()
        val mapItem = mapper(
            MapperOptions(
                defaults = mapOf(
                    "status" to "unknown",
                    "source" to "stream",
                ),
                rename = emptyMap(),
                omit = emptySet(),
            ),
        )

        // Act
        val result = mapItem(item, ctx)

        // Assert
        result shouldBe mapOf(
            "status" to "active",
            "source" to "stream",
            "name" to "Ada",
        )
    }


    //@Test
    fun `mapper transforms existing non-null fields with access to context`(): Unit = runBlocking {
        // Arrange
        val item = mutableMapOf<String, Any?>(
            "name" to "Ada",
            "tenantId" to "tenant-1",
        )
        val ctx = mapOf<String, Any?>(
            "prefix" to "customer",
        )
        val mapItem = mapper(
            MapperOptions(
                rename = emptyMap(),
                omit = emptySet(),
                transform = mapOf(
                    "tenantId" to { value, context -> "${context["prefix"]}#$value" },
                ),
            ),
        )

        // Act
        val result = mapItem(item, ctx)

        // Assert
        result shouldBe mapOf(
            "name" to "Ada",
            "tenantId" to "customer#tenant-1",
        )
    }

    @Test
    fun `mapper does not transform null or missing fields`(): Unit = runBlocking {
        // Arrange
        val item = mutableMapOf<String, Any?>(
            "name" to "Ada",
            "nickname" to null,
        )
        val ctx = emptyMap<String, Any?>()
        val mapItem = mapper(
            MapperOptions(
                rename = emptyMap(),
                omit = emptySet(),
                transform = mapOf(
                    "nickname" to { _, _ -> "transformed-nickname" },
                    "missing" to { _, _ -> "transformed-missing" },
                ),
            ),
        )

        // Act
        val result = mapItem(item, ctx)

        // Assert
        result shouldBe mapOf(
            "name" to "Ada",
            "nickname" to null,
        )
    }

    @Test
    fun `mapper renames transformed values`(): Unit = runBlocking {
        // Arrange
        val item = mutableMapOf<String, Any?>(
            "pk" to "customer|123",
            "name" to "Ada",
        )
        val ctx = emptyMap<String, Any?>()
        val mapItem = mapper(
            MapperOptions(
                rename = mapOf("pk" to "id"),
                omit = setOf("pk"),
                transform = mapOf(
                    "pk" to { value, _ -> sortKeyTransform(value as String) },
                ),
            ),
        )

        // Act
        val result = mapItem(item, ctx)

        // Assert
        result shouldBe mapOf(
            "id" to "123",
            "name" to "Ada",
        )
    }

    @Test
    fun `aggregateMapper places aggregate item fields at the root`(): Unit = runBlocking {
        // Arrange
        val items = listOf(
            mutableMapOf<String, Any?>(
                "discriminator" to "customer",
                "pk" to "customer-1",
                "name" to "Ada",
            ),
        )
        val ctx = emptyMap<String, Any?>()
        val mapItems = aggregateMapper(
            AggregateMapperOptions(
                aggregate = "customer",
                cardinality = emptyMap(),
                mappers = mapOf(
                    "customer" to { item, _ ->
                        mapOf(
                            "id" to item["pk"],
                            "name" to item["name"],
                        )
                    },
                ),
            ),
        )

        // Act
        val result = mapItems(items, ctx)

        // Assert
        result shouldBe mapOf(
            "id" to "customer-1",
            "name" to "Ada",
        )
    }

    @Test
    fun `aggregateMapper nests a single related item under its role`(): Unit = runBlocking {
        // Arrange
        val items = listOf(
            mutableMapOf<String, Any?>(
                "discriminator" to "customer",
                "pk" to "customer-1",
                "name" to "Ada",
            ),
            mutableMapOf<String, Any?>(
                "discriminator" to "address",
                "sk" to "address|shipping",
                "city" to "London",
            ),
        )
        val ctx = emptyMap<String, Any?>()
        val mapItems = aggregateMapper(
            AggregateMapperOptions(
                aggregate = "customer",
                cardinality = mapOf("address" to 1),
                mappers = mapOf(
                    "customer" to { item, _ ->
                        mapOf(
                            "id" to item["pk"],
                            "name" to item["name"],
                        )
                    },
                    "address" to { item, _ ->
                        mapOf(
                            "city" to item["city"],
                        )
                    },
                ),
            ),
        )

        // Act
        val result = mapItems(items, ctx)

        // Assert
        result shouldBe mapOf(
            "id" to "customer-1",
            "name" to "Ada",
            "address" to mapOf(
                "city" to "London",
            ),
        )
    }

    @Test
    fun `aggregateMapper nests multiple related items under their role when cardinality allows many`(): Unit =
        runBlocking {
            // Arrange
            val items = listOf(
                mutableMapOf<String, Any?>(
                    "discriminator" to "customer",
                    "pk" to "customer-1",
                    "name" to "Ada",
                ),
                mutableMapOf<String, Any?>(
                    "discriminator" to "order",
                    "sk" to "order|1",
                    "orderId" to "order-1",
                ),
                mutableMapOf<String, Any?>(
                    "discriminator" to "order",
                    "sk" to "order|2",
                    "orderId" to "order-2",
                ),
            )
            val ctx = emptyMap<String, Any?>()
            val mapItems = aggregateMapper(
                AggregateMapperOptions(
                    aggregate = "customer",
                    cardinality = mapOf("order" to 2),
                    mappers = mapOf(
                        "customer" to { item, _ ->
                            mapOf(
                                "id" to item["pk"],
                                "name" to item["name"],
                            )
                        },
                        "order" to { item, _ ->
                            mapOf(
                                "id" to item["orderId"],
                            )
                        },
                    ),
                ),
            )

            // Act
            val result = mapItems(items, ctx)

            // Assert
            result shouldBe mapOf(
                "id" to "customer-1",
                "name" to "Ada",
                "order" to listOf(
                    mapOf("id" to "order-1"),
                    mapOf("id" to "order-2"),
                ),
            )
        }

    @Test
    fun `aggregateMapper ignores deleted items`(): Unit = runBlocking {
        // Arrange
        val items = listOf(
            mutableMapOf<String, Any?>(
                "discriminator" to "customer",
                "pk" to "customer-1",
                "name" to "Ada",
            ),
            mutableMapOf<String, Any?>(
                "discriminator" to "order",
                "sk" to "order|1",
                "orderId" to "order-1",
                "deleted" to true,
            ),
        )
        val ctx = emptyMap<String, Any?>()
        val mapItems = aggregateMapper(
            AggregateMapperOptions(
                aggregate = "customer",
                cardinality = mapOf("order" to 2),
                mappers = mapOf(
                    "customer" to { item, _ ->
                        mapOf(
                            "id" to item["pk"],
                            "name" to item["name"],
                        )
                    },
                    "order" to { item, _ ->
                        mapOf(
                            "id" to item["orderId"],
                        )
                    },
                ),
            ),
        )

        // Act
        val result = mapItems(items, ctx)

        // Assert
        result shouldBe mapOf(
            "id" to "customer-1",
            "name" to "Ada",
        )
    }

    @Test
    fun `aggregateMapper skips related items without a sort key`(): Unit = runBlocking {
        // Arrange
        val items = listOf(
            mutableMapOf<String, Any?>(
                "discriminator" to "customer",
                "pk" to "customer-1",
                "name" to "Ada",
            ),
            mutableMapOf<String, Any?>(
                "discriminator" to "order",
                "orderId" to "order-1",
            ),
        )
        val ctx = emptyMap<String, Any?>()
        val mapItems = aggregateMapper(
            AggregateMapperOptions(
                aggregate = "customer",
                cardinality = mapOf("order" to 2),
                mappers = mapOf(
                    "customer" to { item, _ ->
                        mapOf(
                            "id" to item["pk"],
                            "name" to item["name"],
                        )
                    },
                    "order" to { item, _ ->
                        mapOf(
                            "id" to item["orderId"],
                        )
                    },
                ),
            ),
        )

        // Act
        val result = mapItems(items, ctx)

        // Assert
        result shouldBe mapOf(
            "id" to "customer-1",
            "name" to "Ada",
        )
    }

    @Test
    fun `aggregateMapper uses the original item when no mapper exists for discriminator`(): Unit = runBlocking {
        // Arrange
        val items = listOf(
            mutableMapOf<String, Any?>(
                "discriminator" to "customer",
                "pk" to "customer-1",
                "name" to "Ada",
            ),
            mutableMapOf<String, Any?>(
                "discriminator" to "note",
                "sk" to "note|1",
                "message" to "hello",
            ),
        )
        val ctx = emptyMap<String, Any?>()
        val mapItems = aggregateMapper(
            AggregateMapperOptions(
                aggregate = "customer",
                cardinality = mapOf("note" to 1),
                mappers = mapOf(
                    "customer" to { item, _ ->
                        mapOf(
                            "id" to item["pk"],
                            "name" to item["name"],
                        )
                    },
                ),
            ),
        )

        // Act
        val result = mapItems(items, ctx)

        // Assert
        result shouldBe mapOf(
            "id" to "customer-1",
            "name" to "Ada",
            "note" to mapOf(
                "discriminator" to "note",
                "sk" to "note|1",
                "message" to "hello",
            ),
        )
    }
}