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
) : KVStore<K> {
    private val storage = sortedMapOf<K, ByteArray>()
    private var closed = false
    private var walFailure: Throwable? = null

    init {
        try {
            wal.replay().forEach { entry ->
                when (val operation = entry.operation) {
                    is WalOperation.Put -> storage[decodeKey(operation.key.copyOf())] = operation.value.copyOf()
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
        persist(WalOperation.Put(encodedKey, stableValue)) {
            storage[key] = stableValue
        }
    }

    @Synchronized
    override fun get(key: K): ByteArray? {
        ensureUsable()
        return storage[key]?.copyOf()
    }

    @Synchronized
    override fun delete(key: K) {
        ensureUsable()
        val encodedKey = encodeKey(key).copyOf()
        persist(WalOperation.Delete(encodedKey)) {
            storage.remove(key)
        }
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
}
