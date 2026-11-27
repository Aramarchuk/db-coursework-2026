package aramarchuk.keyValueBase.storage

import java.nio.file.Path

data class ReplicaConfig(
    val replicaId: Long,
    val port: Int,
    val dataDirectory: Path,
)

/** Encodes and decodes maps to and from binary data. */
interface MapCodec<K, V> {
    fun encode(map: Map<K, V>): ByteArray
    fun decode(bytes: ByteArray): Map<K, V>
}

