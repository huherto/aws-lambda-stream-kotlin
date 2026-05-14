package org.myorg.sut

typealias Item = MutableMap<String, Any?>
typealias Context = Map<String, Any?>
typealias Transform = suspend (value: Any?, ctx: Context) -> Any?
typealias MapperFunction = suspend (item: Item, ctx: Context) -> Map<String, Any?>

fun sortKeyTransform(v: String): String =
    v.split("|")[1]

fun deletedFilter(item: Map<String, Any?>): Boolean =
    item["deleted"] != true

val DEFAULT_OMIT_FIELDS = setOf(
    "pk",
    "sk",
    "data",
    "data2",
    "data3",
    "data4",
    "discriminator",
    "ttl",
    "latched",
    "deleted",
    "pull",
    "awsregion",
    "aws:rep:updateregion",
    "aws:rep:updatetime",
    "aws:rep:deleting",
    "eem",
)

val DEFAULT_RENAME = mapOf(
    "pk" to "id",
)

data class MapperOptions(
    val defaults: Map<String, Any?> = emptyMap(),
    val rename: Map<String, String> = DEFAULT_RENAME,
    val omit: Set<String> = DEFAULT_OMIT_FIELDS,
    val transform: Map<String, Transform> = emptyMap(),
)

fun mapper(
    options: MapperOptions = MapperOptions(),
): MapperFunction = { item, ctx ->
    val transformed = item.toMutableMap()

    for ((key, transformer) in options.transform) {
        val value = item[key]

        if (value != null) {
            transformed[key] = transformer(value, ctx)
        }
    }

    val renamed = item.toMutableMap()

    for ((from, to) in options.rename) {
        val value = transformed[from]

        if (value != null) {
            renamed[to] = value
        }
    }

    buildMap {
        putAll(options.defaults)

        renamed
            .filterKeys { key -> key !in options.omit }
            .forEach { (key, value) -> put(key, value) }
    }
}

data class AggregateMapperOptions(
    val aggregate: String,
    val cardinality: Map<String, Int>,
    val mappers: Map<String, MapperFunction>,
    val delimiter: String = "|",
)

fun aggregateMapper(
    options: AggregateMapperOptions,
): suspend (
    items: List<Item>,
    ctx: Context,
) -> Map<String, Any?> = { items, ctx ->
    items
        .filter(::deletedFilter)
        .fold(mutableMapOf<String, Any?>()) { accumulator, current ->
            val discriminator = current["discriminator"] as? String
            var mapping = options.mappers[discriminator]
            if (mapping == null) {
                mapping = { item: Item, _: Context -> item }
            }

            val mapped = mapping.invoke(current, ctx)

            if (discriminator == options.aggregate) {
                mutableMapOf<String, Any?>().apply {
                    putAll(mapped)
                    putAll(accumulator)
                }
            } else {
                val sortKey = current["sk"] as? String
                    ?: return@fold accumulator

                val role = sortKey.split(options.delimiter)[0]
                val allowsMultiple = (options.cardinality[role] ?: 0) > 1

                val existing = accumulator.computeIfAbsent(role) {
                    if (allowsMultiple) {
                        mutableListOf<Map<String, Any?>>()
                    } else {
                        mapped
                    }
                }

                if (allowsMultiple) {
                    @Suppress("UNCHECKED_CAST")
                    (existing as MutableList<Map<String, Any?>>).add(mapped)
                }

                accumulator
            }
        }
}