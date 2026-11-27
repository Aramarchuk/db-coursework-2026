package aramarchuk.keyValueBase.storage

import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.DataInput
import java.io.DataInputStream
import java.io.DataOutput
import java.io.DataOutputStream
import java.io.IOException

/** Reads and writes exactly one self-delimiting item in a shared binary stream. */
interface ItemCodec<T> {
    fun writeItem(item: T, output: DataOutput)
    fun readItem(input: DataInput): T
}

class MapBinaryCodec<K, V>(
    private val encodeKey: (K) -> ByteArray,
    private val decodeKey: (ByteArray) -> K,
    private val itemCodec: ItemCodec<V>,
) : MapCodec<K, V> {
    override fun encode(map: Map<K, V>): ByteArray {
        val bytes = ByteArrayOutputStream()
        DataOutputStream(bytes).use { output ->
            output.writeByte(FORMAT_VERSION)
            output.writeInt(map.size)
            map.forEach { (key, item) ->
                val encodedKey = encodeKey(key)
                output.writeInt(encodedKey.size)
                output.write(encodedKey)
                itemCodec.writeItem(item, output)
            }
        }
        return bytes.toByteArray()
    }

    override fun decode(bytes: ByteArray): Map<K, V> {
        DataInputStream(ByteArrayInputStream(bytes)).use { input ->
            val formatVersion = input.readUnsignedByte()
            if (formatVersion != FORMAT_VERSION) {
                throw IOException("Unsupported map format version $formatVersion")
            }

            val entryCount = input.readInt()
            if (entryCount < 0 || entryCount > input.available() / Int.SIZE_BYTES) {
                throw IOException("Invalid map entry count $entryCount")
            }

            val entries = linkedMapOf<K, V>()
            val encodedKeys = mutableListOf<ByteArray>()
            repeat(entryCount) {
                val keySize = input.readInt()
                if (keySize < 0 || keySize > input.available()) {
                    throw IOException("Invalid map key size $keySize")
                }

                val keyBytes = ByteArray(keySize)
                input.readFully(keyBytes)
                if (encodedKeys.any { it.contentEquals(keyBytes) }) {
                    throw IOException("Duplicate map key bytes")
                }
                encodedKeys += keyBytes

                val key = decodeKey(keyBytes)
                if (entries.containsKey(key)) throw IOException("Duplicate decoded map key")
                entries[key] = itemCodec.readItem(input)
            }

            if (input.available() != 0) {
                throw IOException("Unexpected trailing bytes in map")
            }
            return entries
        }
    }

    private companion object {
        const val FORMAT_VERSION = 1
    }
}
