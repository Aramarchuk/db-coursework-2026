package aramarchuk.keyValueBase.storage

import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.DataInputStream
import java.io.DataOutputStream
import java.io.IOException

class CounterOperationCodec<K>(
    private val encodeKey: (K) -> ByteArray,
    private val decodeKey: (ByteArray) -> K,
) : WalOperationCodec<CounterOperation<K>> {
    override fun encode(operation: CounterOperation<K>): ByteArray {
        val bytes = ByteArrayOutputStream()
        DataOutputStream(bytes).use { output ->
            output.writeByte(FORMAT_VERSION)
            when (operation) {
                is CounterOperation.Increment -> {
                    output.writeByte(INCREMENT)
                    writeBytes(output, encodeKey(operation.key))
                }
                is CounterOperation.Decrement -> {
                    output.writeByte(DECREMENT)
                    writeBytes(output, encodeKey(operation.key))
                }
                is CounterOperation.Merge -> {
                    output.writeByte(MERGE)
                    writeBytes(output, encodeKey(operation.key))
                    writeCounter(output, operation.counter)
                }
            }
        }
        return bytes.toByteArray()
    }

    override fun decode(bytes: ByteArray): CounterOperation<K> {
        DataInputStream(ByteArrayInputStream(bytes)).use { input ->
            val version = input.readUnsignedByte()
            if (version != FORMAT_VERSION) throw IOException("Unsupported counter WAL operation version $version")

            val type = input.readUnsignedByte()
            val key = decodeKey(readBytes(input, "key"))
            val operation = when (type) {
                INCREMENT -> CounterOperation.Increment(key)
                DECREMENT -> CounterOperation.Decrement(key)
                MERGE -> CounterOperation.Merge(key, readCounter(input))
                else -> throw IOException("Unknown counter WAL operation type $type")
            }
            if (input.available() != 0) throw IOException("Unexpected trailing bytes in counter WAL operation")
            return operation
        }
    }

    private fun writeCounter(output: DataOutputStream, counter: Counter) {
        val components = counter.counters.toSortedMap()
        output.writeInt(components.size)
        components.forEach { (replicaId, pair) ->
            output.writeLong(replicaId)
            output.writeLong(pair.increments)
            output.writeLong(pair.decrements)
        }
    }

    private fun readCounter(input: DataInputStream): Counter {
        val componentCount = input.readInt()
        if (componentCount < 0 || componentCount > input.available() / COMPONENT_SIZE) {
            throw IOException("Invalid counter component count $componentCount")
        }

        val components = sortedMapOf<Long, PNPair>()
        repeat(componentCount) {
            val replicaId = input.readLong()
            val increments = input.readLong()
            val decrements = input.readLong()
            if (replicaId < 0L || increments < 0L || decrements < 0L) {
                throw IOException("Invalid PN-counter component ($replicaId, $increments, $decrements)")
            }
            if (components.put(replicaId, PNPair(increments, decrements)) != null) {
                throw IOException("Duplicate PN-counter replica ID $replicaId")
            }
        }
        return Counter(components)
    }

    private fun writeBytes(output: DataOutputStream, bytes: ByteArray) {
        output.writeInt(bytes.size)
        output.write(bytes)
    }

    private fun readBytes(input: DataInputStream, field: String): ByteArray {
        val size = input.readInt()
        if (size < 0 || size > input.available()) throw IOException("Invalid counter WAL $field size $size")
        return ByteArray(size).also(input::readFully)
    }

    private companion object {
        const val FORMAT_VERSION = 1
        const val INCREMENT = 1
        const val DECREMENT = 2
        const val MERGE = 3
        const val COMPONENT_SIZE = Long.SIZE_BYTES * 3
    }
}
