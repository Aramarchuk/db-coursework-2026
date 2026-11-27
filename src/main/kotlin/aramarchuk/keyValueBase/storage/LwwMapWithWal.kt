package aramarchuk.keyValueBase.storage

/**
 * In-memory key-value state backed by a write-ahead log. The state is rebuilt
 * from the log when this store is opened.
 *
 * Only one store should use a given [WriteAheadLog] at a time. Key codecs must
 * be stable across restarts.
 */
class LwwMapWithWal<K : Comparable<K>>(
    wal: WriteAheadLog,
    private val encodeKey: (K) -> ByteArray,
    private val decodeKey: (ByteArray) -> K,
    override val replicaId: Long,
) : KVStore<K>, AutoCloseable, LwwMap<K, ByteArray> {
    private val stateStore = WalBackedStateStore(
        wal = wal,
        initialState = sortedMapOf<K, LwwRecord<ByteArray>>(),
        applyOperation = { currentState, operation ->
            val nextState = sortedMapOf<K, LwwRecord<ByteArray>>().apply { putAll(currentState) }
            when (operation) {
                is WalOperation.Put -> {
                    val key = decodeKey(operation.key.copyOf())
                    nextState[key] = LwwRecordCodec.decode(operation.value.copyOf())
                }
                is WalOperation.Delete -> nextState.remove(decodeKey(operation.key.copyOf()))
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
        val encodedKey = encodeKey(key).copyOf()
        val stableValue = value.copyOf()
        val record = LwwRecord.Value(stableValue, LwwVersion(nextLogicalClock(), replicaId))
        stateStore.apply(WalOperation.Put(encodedKey, LwwRecordCodec.encode(record)))
    }

    @Synchronized
    override fun get(key: K): ByteArray? {
        ensureUsable()
        return (stateStore.state()[key] as? LwwRecord.Value)?.value?.copyOf()
    }

    @Synchronized
    override fun delete(key: K) {
        ensureUsable()
        val encodedKey = encodeKey(key).copyOf()
        val record = LwwRecord.Tombstone(LwwVersion(nextLogicalClock(), replicaId))
        stateStore.apply(WalOperation.Put(encodedKey, LwwRecordCodec.encode(record)))
    }

    @Synchronized
    override fun merge(state: LwwMapState<K, ByteArray>) {
        ensureUsable()
        state.entries.forEach { (key, incomingRecord) ->
            logicalClock = maxOf(logicalClock, incomingRecord.version.logicalClock)
            val currentRecord = stateStore.state()[key]
            if (currentRecord == null || incomingRecord.version > currentRecord.version) {
                val stableRecord = copyRecord(incomingRecord)
                val encodedKey = encodeKey(key).copyOf()
                stateStore.apply(WalOperation.Put(encodedKey, LwwRecordCodec.encode(stableRecord)))
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
