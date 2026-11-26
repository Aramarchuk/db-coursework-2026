package aramarchuk.keyValueBase.storage

data class LwwVersion(
    val logicalClock: Long,
    val replicaId: Long,
) : Comparable<LwwVersion> {
    override fun compareTo(other: LwwVersion): Int =
        compareValuesBy(this, other, LwwVersion::logicalClock, LwwVersion::replicaId)
}

sealed interface LwwRecord<out V> {
    val version: LwwVersion

    data class Value<V>(
        val value: V,
        override val version: LwwVersion,
    ) : LwwRecord<V>

    data class Tombstone(
        override val version: LwwVersion,
    ) : LwwRecord<Nothing>
}

data class LwwMapState<K, V>(
    val entries: Map<K, LwwRecord<V>>,
)

/** Local key-value operations and state-based CRDT merge. */
interface LwwMap<K, V> : AutoCloseable {
    val replicaId: Long

    fun get(key: K): V?
    fun put(key: K, value: V)
    fun delete(key: K)

    /** Merge a peer's full snapshot into this map. */
    fun merge(state: LwwMapState<K, V>)

    /** Export values and tombstones for synchronization. */
    fun snapshot(): LwwMapState<K, V>
}
