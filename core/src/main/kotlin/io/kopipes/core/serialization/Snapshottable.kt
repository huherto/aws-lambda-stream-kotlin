package io.kopipes.core.serialization

/**
 * Represents an entity that can provide a simplified, serializable snapshot of its internal state.
 *
 * This interface is typically implemented by pipeline extensions, context structures, or third-party
 * request/response wrappers to produce human-readable or structured diagnostic representations (e.g., [Map]
 * or primitive values) suitable for inclusion in unit-of-work snapshots, auditing, and diagnostic logs.
 */
interface Snapshottable {
    /**
     * Converts the current state of this object into a snapshot representation suitable for serialization.
     *
     * @return a serializable object, map, or primitive representing the state, or `null`.
     */
    fun toSnapshot(): Any?
}
