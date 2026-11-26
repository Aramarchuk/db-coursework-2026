package aramarchuk.keyValueBase.storage

/**
 * In-memory key-value state backed by a write-ahead log. The state is rebuilt
 * from the log when this store is opened.
 *
 * Only one store should use a given [WriteAheadLog] at a time. Key codecs must
 * be stable across restarts.
 */
class WalBackedKVStore<K : Comparable<K>>(
    private val wal: WriteAheadLog,
    private val encodeKey: (K) -> ByteArray,
    private val decodeKey: (ByteArray) -> K,
    override val replicaId: Long,
) : KVStore<K>, AutoCloseable, LwwMap<K, ByteArray> {
    private val storage = sortedMapOf<K, LwwRecord<ByteArray>>()
    private var logicalClock = 0L
    private var closed = false
    private var walFailure: Throwable? = null

    init {
        try {
            wal.replay().forEach { entry ->
                when (val operation = entry.operation) {
                    is WalOperation.Put -> {
                        val record = LwwRecordCodec.decode(operation.value)
                        storage[decodeKey(operation.key.copyOf())] = record
                        logicalClock = maxOf(logicalClock, record.version.logicalClock)
                    }
                    is WalOperation.Delete -> storage.remove(decodeKey(operation.key.copyOf()))
                }
            }
        } catch (failure: Throwable) {
            try {
                wal.close()
            } catch (closeFailure: Throwable) {
                failure.addSuppressed(closeFailure)
            }
            throw failure
        }
    }

    @Synchronized
    override fun put(key: K, value: ByteArray) {
        ensureUsable()
        val encodedKey = encodeKey(key).copyOf()
        val stableValue = value.copyOf()
        val record = LwwRecord.Value(stableValue, LwwVersion(nextLogicalClock(), replicaId))
        persist(WalOperation.Put(encodedKey, LwwRecordCodec.encode(record))) {
            storage[key] = record
        }
    }

    @Synchronized
    override fun get(key: K): ByteArray? {
        ensureUsable()
        return (storage[key] as? LwwRecord.Value)?.value?.copyOf()
    }

    @Synchronized
    override fun delete(key: K) {
        ensureUsable()
        val encodedKey = encodeKey(key).copyOf()
        val record = LwwRecord.Tombstone(LwwVersion(nextLogicalClock(), replicaId))
        persist(WalOperation.Put(encodedKey, LwwRecordCodec.encode(record))) {
            storage[key] = record
        }
    }

    @Synchronized
    override fun merge(state: LwwMapState<K, ByteArray>) {
        ensureUsable()
        state.entries.forEach { (key, incomingRecord) ->
            logicalClock = maxOf(logicalClock, incomingRecord.version.logicalClock)
            val currentRecord = storage[key]
            if (currentRecord == null || incomingRecord.version > currentRecord.version) {
                val stableRecord = copyRecord(incomingRecord)
                val encodedKey = encodeKey(key).copyOf()
                persist(WalOperation.Put(encodedKey, LwwRecordCodec.encode(stableRecord))) {
                    storage[key] = stableRecord
                }
            }
        }
    }

    @Synchronized
    override fun snapshot(): LwwMapState<K, ByteArray> {
        ensureUsable()
        val entries = sortedMapOf<K, LwwRecord<ByteArray>>()
        storage.forEach { (key, record) -> entries[key] = copyRecord(record) }
        return LwwMapState(entries)
    }

    @Synchronized
    override fun close() {
        if (closed) return
        closed = true
        wal.close()
    }

    private fun persist(operation: WalOperation, updateMemory: () -> Unit) {
        try {
            wal.append(operation)
            wal.sync()
        } catch (failure: Throwable) {
            // After a WAL error, its durable state may be uncertain. Require a
            // reopen and replay before serving more reads or writes.
            walFailure = failure
            throw failure
        }
        updateMemory()
    }

    private fun ensureUsable() {
        check(!closed) { "Store is closed" }
        walFailure?.let { failure ->
            throw IllegalStateException("Store is unusable after a WAL failure; reopen it", failure)
        }
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
