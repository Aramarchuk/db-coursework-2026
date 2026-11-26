package aramarchuk.keyValueBase.storage

import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.DataInputStream
import java.io.DataOutputStream
import java.io.DataInput
import java.io.DataOutput
import java.io.IOException

object LwwRecordCodec {
    fun encode(record: LwwRecord<ByteArray>): ByteArray {
        val bytes = ByteArrayOutputStream()
        DataOutputStream(bytes).use { output ->
            writeRecordTo(record, output)
        }
        return bytes.toByteArray()
    }

    internal fun writeRecordTo(record: LwwRecord<ByteArray>, output: DataOutput) {
        output.writeByte(FORMAT_VERSION)
        when (record) {
            is LwwRecord.Value -> {
                output.writeByte(VALUE_RECORD)
                writeVersion(output, record.version)
                output.writeInt(record.value.size)
                output.write(record.value)
            }

            is LwwRecord.Tombstone -> {
                output.writeByte(TOMBSTONE_RECORD)
                writeVersion(output, record.version)
            }
        }
    }

    fun decode(bytes: ByteArray): LwwRecord<ByteArray> {
        DataInputStream(ByteArrayInputStream(bytes)).use { input ->
            val record = decode(input)
            if (input.available() != 0) {
                throw IOException("Unexpected trailing bytes in LWW record")
            }
            return record
        }
    }

    fun decode(input: DataInput): LwwRecord<ByteArray> {
        val formatVersion = input.readUnsignedByte()
        if (formatVersion != FORMAT_VERSION) {
            throw IOException("Unsupported LWW record format version $formatVersion")
        }

        val recordType = input.readUnsignedByte()
        val version = readVersion(input)
        return when (recordType) {
            VALUE_RECORD -> {
                val valueSize = input.readInt()
                if (valueSize < 0) throw IOException("Invalid LWW value size $valueSize")
                val value = ByteArray(valueSize)
                input.readFully(value)
                LwwRecord.Value(value, version)
            }

            TOMBSTONE_RECORD -> LwwRecord.Tombstone(version)
            else -> throw IOException("Unknown LWW record type $recordType")
        }
    }

    private fun writeVersion(output: DataOutput, version: LwwVersion) {
        output.writeLong(version.logicalClock)
        output.writeLong(version.replicaId)
    }

    private fun readVersion(input: DataInput): LwwVersion {
        val logicalClock = input.readLong()
        val replicaId = input.readLong()
        if (logicalClock < 0L || replicaId < 0L) {
            throw IOException("Invalid LWW version ($logicalClock, $replicaId)")
        }
        return LwwVersion(logicalClock, replicaId)
    }

    private const val FORMAT_VERSION = 1
    private const val VALUE_RECORD = 1
    private const val TOMBSTONE_RECORD = 2
}
