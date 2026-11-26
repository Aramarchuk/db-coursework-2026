package aramarchuk.keyValueBase.storage

import java.nio.file.Path

data class ReplicaConfig(
    val replicaId: Long,
    val port: Int,
    val dataDirectory: Path,
)

/** Encodes and decodes a full state snapshot for transport. */
interface LwwMapStateCodec<K, V> {
    fun encode(state: LwwMapState<K, V>): ByteArray
    fun decode(bytes: ByteArray): LwwMapState<K, V>
}

