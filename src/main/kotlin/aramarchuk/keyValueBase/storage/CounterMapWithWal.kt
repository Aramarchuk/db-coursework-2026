package aramarchuk.keyValueBase.storage

sealed interface CounterOperation<K> {
    val key: K

    data class Increment<K>(override val key: K) : CounterOperation<K>
    data class Decrement<K>(override val key: K) : CounterOperation<K>
    data class Merge<K>(override val key: K, val counter: Counter) : CounterOperation<K>
}

/** A state-based PN-counter map persisted through the shared WAL state store. */
class CounterMapWithWal<K : Comparable<K>>(
    override val replicaId: Long,
    wal: WriteAheadLog<CounterOperation<K>>,
) : AutoCloseable, CounterMap<K> {
    init {
        require(replicaId >= 0L) { "Replica ID must be non-negative" }
    }

    private val stateStore = WalBackedStateStore<Map<K, Counter>, CounterOperation<K>>(
        wal = wal,
        initialState = sortedMapOf(),
        applyOperation = ::applyOperation,
    )

    @Synchronized
    override fun get(key: K): Long = sumOf(stateStore.state()[key] ?: Counter())

    @Synchronized
    override fun increment(key: K) {
        stateStore.apply(CounterOperation.Increment(key))
    }

    @Synchronized
    override fun decrement(key: K) {
        stateStore.apply(CounterOperation.Decrement(key))
    }

    @Synchronized
    override fun merge(state: CounterMapState<K>) {
        state.entries.forEach { (key, incomingCounter) ->
            val currentCounter = stateStore.state()[key] ?: Counter()
            val mergedCounter = mergeCounters(currentCounter, incomingCounter)
            if (mergedCounter != currentCounter) {
                stateStore.apply(CounterOperation.Merge(key, copyCounter(incomingCounter)))
            }
        }
    }

    @Synchronized
    override fun snapshot(): CounterMapState<K> {
        val entries = sortedMapOf<K, Counter>()
        stateStore.state().forEach { (key, counter) -> entries[key] = copyCounter(counter) }
        return CounterMapState(entries)
    }

    override fun sumOf(counter: Counter): Long = counter.counters.values.fold(0L) { sum, pair ->
        Math.subtractExact(Math.addExact(sum, pair.increments), pair.decrements)
    }

    @Synchronized
    override fun close() = stateStore.close()

    private fun applyOperation(
        currentState: Map<K, Counter>,
        operation: CounterOperation<K>,
    ): Map<K, Counter> {
        val key = operation.key
        val currentCounter = currentState[key] ?: Counter()
        val nextCounter = when (operation) {
            is CounterOperation.Increment -> adjustLocalCounter(currentCounter, increment = true)
            is CounterOperation.Decrement -> adjustLocalCounter(currentCounter, increment = false)
            is CounterOperation.Merge -> mergeCounters(currentCounter, operation.counter)
        }
        if (nextCounter == currentCounter) return currentState

        return sortedMapOf<K, Counter>().apply {
            putAll(currentState)
            put(key, nextCounter)
        }
    }

    private fun adjustLocalCounter(counter: Counter, increment: Boolean): Counter {
        val currentPair = counter.counters[replicaId] ?: PNPair()
        val nextPair = if (increment) {
            currentPair.copy(increments = Math.addExact(currentPair.increments, 1L))
        } else {
            currentPair.copy(decrements = Math.addExact(currentPair.decrements, 1L))
        }
        return Counter(counter.counters + (replicaId to nextPair))
    }

    private fun mergeCounters(left: Counter, right: Counter): Counter {
        val merged = sortedMapOf<Long, PNPair>()
        (left.counters.keys + right.counters.keys).forEach { id ->
            val leftPair = left.counters[id] ?: PNPair()
            val rightPair = right.counters[id] ?: PNPair()
            val pair = PNPair(
                increments = maxOf(leftPair.increments, rightPair.increments),
                decrements = maxOf(leftPair.decrements, rightPair.decrements),
            )
            if (pair.increments != 0L || pair.decrements != 0L) merged[id] = pair
        }
        return Counter(merged)
    }

    private fun copyCounter(counter: Counter): Counter = Counter(counter.counters.toMap())
}
