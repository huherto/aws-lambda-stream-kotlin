package io.kopipes.core.utils

/** Interface for reading AttributeValue maps. */
interface AttributeValueMapReader {
    fun getS(fieldName: String): String?
    fun getDouble(fieldName: String): Double?
    fun getInt(fieldName: String): Int?
    fun getBool(fieldName: String): Boolean?
    fun getLong(fieldName: String): Long?
    fun isNull(fieldName: String): Boolean?
}
