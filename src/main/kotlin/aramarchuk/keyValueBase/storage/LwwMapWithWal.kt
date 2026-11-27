package aramarchuk.keyValueBase.storage

/**
 * In-memory key-value state backed by a write-ahead log. The state is rebuilt
 * from the log when this store is opened.
 *
 * Only one store should use a given [WriteAheadLog] at a time. The WAL's key
 * codec must remain stable across restarts.
 */
class LwwMapWithWal<K : Comparable<K>>(
    wal: WriteAheadLog<LwwWalOperation<K>>,
    override val replicaId: Long,
) : KVStore<K>, AutoCloseable, LwwMap<K, ByteArray> {
    private val stateStore = WalBackedStateStore<Map<K, LwwRecord<ByteArray>>, LwwWalOperation<K>>(
        wal = wal,
        initialState = sortedMapOf<K, LwwRecord<ByteArray>>(),
        applyOperation = { currentState, operation ->
            val nextState = sortedMapOf<K, LwwRecord<ByteArray>>().apply { putAll(currentState) }
            when (operation) {
                is LwwWalOperation.SetRecord -> nextState[operation.key] = copyRecord(operation.record)
                is LwwWalOperation.Remove -> nextState.remove(operation.key)
            }
            nextState
        },
    )
    private var logicalClock = 0L

    init {
        stateStore.state().values.forEach { record ->
            logicalClock = maxOf(logicalClock, record.version.logicalClock)
        }
    }

    @Synchronized
    override fun put(key: K, value: ByteArray) {
        ensureUsable()
        val stableValue = value.copyOf()
        val record = LwwRecord.Value(stableValue, LwwVersion(nextLogicalClock(), replicaId))
        stateStore.apply(LwwWalOperation.SetRecord(key, record))
    }

    @Synchronized
    override fun get(key: K): ByteArray? {
        ensureUsable()
        return (stateStore.state()[key] as? LwwRecord.Value)?.value?.copyOf()
    }

    @Synchronized
    override fun delete(key: K) {
        ensureUsable()
        val record = LwwRecord.Tombstone(LwwVersion(nextLogicalClock(), replicaId))
        stateStore.apply(LwwWalOperation.SetRecord(key, record))
    }

    @Synchronized
    override fun merge(state: LwwMapState<K, ByteArray>) {
        ensureUsable()
        state.entries.forEach { (key, incomingRecord) ->
            logicalClock = maxOf(logicalClock, incomingRecord.version.logicalClock)
            val currentRecord = stateStore.state()[key]
            if (currentRecord == null || incomingRecord.version > currentRecord.version) {
                val stableRecord = copyRecord(incomingRecord)
                stateStore.apply(LwwWalOperation.SetRecord(key, stableRecord))
            }
        }
    }

    @Synchronized
    override fun snapshot(): LwwMapState<K, ByteArray> {
        ensureUsable()
        val entries = sortedMapOf<K, LwwRecord<ByteArray>>()
        stateStore.state().forEach { (key, record) -> entries[key] = copyRecord(record) }
        return LwwMapState(entries)
    }

    @Synchronized
    override fun close() {
        stateStore.close()
    }

    private fun ensureUsable() {
        // The generic store owns closed and WAL failure checks.
        stateStore.state()
    }

    private fun nextLogicalClock(): Long {
        logicalClock = Math.addExact(logicalClock, 1L)
        return logicalClock
    }

    private fun copyRecord(record: LwwRecord<ByteArray>): LwwRecord<ByteArray> = when (record) {
        is LwwRecord.Value -> LwwRecord.Value(record.value.copyOf(), record.version)
        is LwwRecord.Tombstone -> record
    }
}
