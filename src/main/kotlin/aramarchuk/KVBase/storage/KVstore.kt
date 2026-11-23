package aramarchuk.KVBase.storage

interface KVStore<K> {
    fun put(key: K, value: ByteArray)
    fun get(key: K): ByteArray?
    fun delete(key: K)
    fun close()
}