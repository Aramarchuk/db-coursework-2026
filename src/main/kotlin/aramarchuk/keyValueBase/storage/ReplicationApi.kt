package aramarchuk.keyValueBase.storage

import java.net.URI
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

/** Starts the peer endpoint that merges a posted snapshot and returns merged state. */
interface LwwMapSyncServer : AutoCloseable {
    fun start()
}

/** Exchanges full map state with one peer and merges the response locally. */
interface LwwMapSyncClient {
    fun sync(peer: URI)
}
