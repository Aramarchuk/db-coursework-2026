package aramarchuk.keyValueBase.storage

import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.DataInputStream
import java.io.DataOutputStream
import java.io.IOException

sealed interface LwwWalOperation<K> {
    data class SetRecord<K>(val key: K, val record: LwwRecord<ByteArray>) : LwwWalOperation<K>
    data class Remove<K>(val key: K) : LwwWalOperation<K>
}

/** Binary format for LWW operations in the WAL. */
class LwwWalOperationCodec<K>(
    private val encodeKey: (K) -> ByteArray,
    private val decodeKey: (ByteArray) -> K,
) : WalOperationCodec<LwwWalOperation<K>> {
    override fun encode(operation: LwwWalOperation<K>): ByteArray {
        val bytes = ByteArrayOutputStream()
        DataOutputStream(bytes).use { output ->
            output.writeByte(FORMAT_VERSION)
            when (operation) {
                is LwwWalOperation.SetRecord -> {
                    output.writeByte(SET_RECORD)
                    writeBytes(output, encodeKey(operation.key))
                    writeBytes(output, LwwRecordCodec.encode(operation.record))
                }
                is LwwWalOperation.Remove -> {
                    output.writeByte(REMOVE)
                    writeBytes(output, encodeKey(operation.key))
                }
            }
        }
        return bytes.toByteArray()
    }

    override fun decode(bytes: ByteArray): LwwWalOperation<K> {
        DataInputStream(ByteArrayInputStream(bytes)).use { input ->
            val version = input.readUnsignedByte()
            if (version != FORMAT_VERSION) throw IOException("Unsupported LWW WAL operation version $version")
            val operation = when (val type = input.readUnsignedByte()) {
                SET_RECORD -> LwwWalOperation.SetRecord(
                    decodeKey(readBytes(input, "key")),
                    LwwRecordCodec.decode(readBytes(input, "record")),
                )
                REMOVE -> LwwWalOperation.Remove(decodeKey(readBytes(input, "key")))
                else -> throw IOException("Unknown LWW WAL operation type $type")
            }
            if (input.available() != 0) throw IOException("Unexpected trailing bytes in LWW WAL operation")
            return operation
        }
    }

    private fun writeBytes(output: DataOutputStream, bytes: ByteArray) {
        output.writeInt(bytes.size)
        output.write(bytes)
    }

    private fun readBytes(input: DataInputStream, field: String): ByteArray {
        val size = input.readInt()
        if (size < 0 || size > input.available()) throw IOException("Invalid LWW WAL $field size $size")
        return ByteArray(size).also(input::readFully)
    }

    private companion object {
        const val FORMAT_VERSION = 1
        const val SET_RECORD = 1
        const val REMOVE = 2
    }
}
