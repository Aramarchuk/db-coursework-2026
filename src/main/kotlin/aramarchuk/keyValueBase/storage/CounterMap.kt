package aramarchuk.keyValueBase.storage

/** Per-replica positive and negative components of a PN-counter. */
data class PNPair(
    val increments: Long = 0L,
    val decrements: Long = 0L,
) {
    init {
        require(increments >= 0L && decrements >= 0L) { "PN-counter components must be non-negative" }
    }
}

/** The component map is keyed by stable replica IDs, so replica IDs need not be dense array indexes. */
data class Counter(val counters: Map<Long, PNPair> = emptyMap()) {
    init {
        require(counters.keys.all { it >= 0L }) { "Replica IDs must be non-negative" }
    }
}

data class CounterMapState<K>(val entries: Map<K, Counter>)

interface CounterMap<K> : AutoCloseable {
    val replicaId: Long

    fun get(key: K): Long
    fun increment(key: K)
    fun decrement(key: K)

    fun merge(state: CounterMapState<K>)

    fun snapshot(): CounterMapState<K>
    fun sumOf(counter: Counter): Long
}
