package aramarchuk.keyValueBase.storage

class InMemoryKVStore<K : Comparable<K>>: KVStore<K> {
    val storage = sortedMapOf<K, ByteArray>()
    override fun put(key: K, value: ByteArray) {
        storage[key] = value
    }

    override fun get(key: K): ByteArray? {
        return storage[key]
    }

    override fun delete(key: K) {
        storage.remove(key)
    }

    override fun close() {
        storage.clear()
    }
}
