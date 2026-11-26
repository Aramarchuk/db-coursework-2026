package aramarchuk.keyValueBase.storage

import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.DataInputStream
import java.io.DataOutputStream
import java.io.IOException

class LwwMapStateBinaryCodec<K>(
    private val encodeKey: (K) -> ByteArray,
    private val decodeKey: (ByteArray) -> K,
) : LwwMapStateCodec<K, ByteArray> {
    override fun encode(state: LwwMapState<K, ByteArray>): ByteArray {
        val bytes = ByteArrayOutputStream()
        DataOutputStream(bytes).use { output ->
            output.writeByte(FORMAT_VERSION)
            output.writeInt(state.entries.size)
            state.entries.forEach { (key, record) ->
                val encodedKey = encodeKey(key)
                output.writeInt(encodedKey.size)
                output.write(encodedKey)
                LwwRecordCodec.writeRecordTo(record, output)
            }
        }
        return bytes.toByteArray()
    }

    override fun decode(bytes: ByteArray): LwwMapState<K, ByteArray> {
        DataInputStream(ByteArrayInputStream(bytes)).use { input ->
            val formatVersion = input.readUnsignedByte()
            if (formatVersion != FORMAT_VERSION) {
                throw IOException("Unsupported LWW state format version $formatVersion")
            }

            val entryCount = input.readInt()
            if (entryCount < 0 || entryCount > input.available() / MIN_ENTRY_SIZE) {
                throw IOException("Invalid LWW state entry count $entryCount")
            }

            val entries = linkedMapOf<K, LwwRecord<ByteArray>>()
            val encodedKeys = mutableListOf<ByteArray>()
            repeat(entryCount) {
                val keySize = input.readInt()
                if (keySize < 0 || keySize > input.available() - MIN_RECORD_SIZE) {
                    throw IOException("Invalid LWW key size $keySize")
                }

                val keyBytes = ByteArray(keySize)
                input.readFully(keyBytes)
                if (encodedKeys.any { it.contentEquals(keyBytes) }) {
                    throw IOException("Duplicate LWW state key")
                }
                encodedKeys += keyBytes

                val key = decodeKey(keyBytes)
                if (entries.containsKey(key)) throw IOException("Duplicate decoded LWW state key")
                entries[key] = LwwRecordCodec.decode(input)
            }

            if (input.available() != 0) {
                throw IOException("Unexpected trailing bytes in LWW state")
            }
            return LwwMapState(entries)
        }
    }

    private companion object {
        const val FORMAT_VERSION = 1
        const val MIN_RECORD_SIZE = 18
        const val MIN_ENTRY_SIZE = Int.SIZE_BYTES + MIN_RECORD_SIZE
    }
}
