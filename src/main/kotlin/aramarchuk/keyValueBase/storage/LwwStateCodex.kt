package aramarchuk.keyValueBase.storage

import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.DataInputStream
import java.io.DataOutputStream
import java.io.IOException

object LwwStateCodex: LwwMapStateCodec<ByteArray, ByteArray> {
    private fun encodeItem(k: ByteArray, v: LwwRecord<ByteArray>): ByteArray {
        val bytes = ByteArrayOutputStream()
        DataOutputStream(bytes).use { output ->
            output.writeInt(k.size)
            output.write(k)
            output.write(LwwRecordCodec.encode(v))
        }
        return bytes.toByteArray()
    }
    override fun encode(state: LwwMapState<ByteArray, ByteArray>): ByteArray {
        val bytes = ByteArrayOutputStream()
        DataOutputStream(bytes).use { output ->
            output.writeByte(FORMAT_VERSION)
            output.writeInt(state.entries.size)
            state.entries.forEach { (k, record) -> output.write(encodeItem(k, record))}
        }
        return bytes.toByteArray()
    }

    override fun decode(bytes: ByteArray): LwwMapState<ByteArray, ByteArray> {
        DataInputStream(ByteArrayInputStream(bytes)).use { input ->
            val formatVersion = input.readUnsignedByte()
            if (formatVersion != FORMAT_VERSION) {
                throw IOException("Unsupported LWW state format version $formatVersion")
            }

            val entryCount = input.readInt()
            if (entryCount < 0 || entryCount > input.available() / MIN_ENTRY_SIZE) {
                throw IOException("Invalid LWW state entry count $entryCount")
            }

            val entries = linkedMapOf<ByteArray, LwwRecord<ByteArray>>()
            val keys = mutableListOf<ByteArray>()
            repeat(entryCount) {
                val keySize = input.readInt()
                if (keySize < 0 || keySize > input.available() - MIN_RECORD_SIZE) {
                    throw IOException("Invalid LWW key size $keySize")
                }

                val key = ByteArray(keySize)
                input.readFully(key)
                if (keys.any { it.contentEquals(key) }) {
                    throw IOException("Duplicate LWW state key")
                }
                keys += key
                entries[key] = LwwRecordCodec.decode(input)
            }

            if (input.available() != 0) {
                throw IOException("Unexpected trailing bytes in LWW state")
            }
            return LwwMapState(entries)
        }
    }

    private const val FORMAT_VERSION = 1
    private const val MIN_RECORD_SIZE = 18
    private const val MIN_ENTRY_SIZE = Int.SIZE_BYTES + MIN_RECORD_SIZE
}
