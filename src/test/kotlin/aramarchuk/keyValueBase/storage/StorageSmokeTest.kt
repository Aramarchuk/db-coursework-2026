package aramarchuk.keyValueBase.storage

import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.DataInputStream
import java.io.DataOutputStream
import java.nio.channels.FileChannel
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption.WRITE
import java.util.Comparator
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.fail

class StorageSmokeTest {
    private sealed interface TestWalOperation {
        data class Put(val key: ByteArray, val value: ByteArray) : TestWalOperation {
            override fun equals(other: Any?): Boolean =
                this === other || (other is Put && key.contentEquals(other.key) && value.contentEquals(other.value))

            override fun hashCode(): Int = 31 * key.contentHashCode() + value.contentHashCode()
        }

        data class Delete(val key: ByteArray) : TestWalOperation {
            override fun equals(other: Any?): Boolean = this === other || (other is Delete && key.contentEquals(other.key))
            override fun hashCode(): Int = key.contentHashCode()
        }
    }

    @Test
    fun `LWW record codec round trips values and tombstones`() {
        val state = LwwMapState(
            linkedMapOf(
                "value" to LwwRecord.Value(bytes("record value"), LwwVersion(7L, 3L)),
                "removed" to LwwRecord.Tombstone(LwwVersion(8L, 2L)),
            ),
        )
        val decodedEntries = state.entries.mapValues { (_, record) ->
            LwwRecordCodec.decode(LwwRecordCodec.encode(record))
        }

        assertStatesEqual(state, LwwMapState(decodedEntries))
    }

    @Test
    fun `map binary codec round trips arbitrary item types`() {
        val codec = MapBinaryCodec(
            encodeKey = { it.toByteArray(Charsets.UTF_8) },
            decodeKey = { it.toString(Charsets.UTF_8) },
            itemCodec = object : ItemCodec<Int> {
                override fun writeItem(item: Int, output: java.io.DataOutput) {
                    output.writeInt(item)
                }

                override fun readItem(input: java.io.DataInput): Int = input.readInt()
            },
        )
        val map = linkedMapOf("zero" to 0, "negative" to -12, "positive" to 345)

        assertEquals(map, codec.decode(codec.encode(map)))
    }

    @Test
    fun `state codec round trips values and tombstones`() {
        val state = LwwMapState(
            linkedMapOf(
                "a" to LwwRecord.Value(bytes("short"), LwwVersion(3L, 1L)),
                "a much longer key" to LwwRecord.Tombstone(LwwVersion(8L, 2L)),
            ),
        )

        val codec = stringStateCodec()
        assertStatesEqual(state, LwwMapState(codec.decode(codec.encode(state.entries))))
    }

    @Test
    fun `state survives writing to a file and loading into a new in-memory state`() =
        withTempDirectory { directory ->
            val state = LwwMapState(
                linkedMapOf(
                    "alpha" to LwwRecord.Value(bytes("first value"), LwwVersion(4L, 1L)),
                    "beta" to LwwRecord.Value(bytes("a longer value"), LwwVersion(5L, 2L)),
                    "removed" to LwwRecord.Tombstone(LwwVersion(6L, 2L)),
                ),
            )
            val stateFile = directory.resolve("snapshot.bin")
            val codec = stringStateCodec()

            Files.write(stateFile, codec.encode(state.entries))
            val restoredState = LwwMapState(codec.decode(Files.readAllBytes(stateFile)))

            assertStatesEqual(state, restoredState)
        }

    @Test
    fun `WAL replays operations after reopening`() = withTempDirectory { directory ->
        val walPath = directory.resolve("store.wal")

        FileWriteAheadLog(walPath, walOperationCodec()).use { wal ->
            assertEquals(1L, wal.append(TestWalOperation.Put(bytes("key"), bytes("value"))))
            assertEquals(2L, wal.append(TestWalOperation.Delete(bytes("other"))))
            wal.sync()
        }

        FileWriteAheadLog(walPath, walOperationCodec()).use { wal ->
            val entries = wal.replay().toList()
            assertEquals(listOf(1L, 2L), entries.map { it.lsn })
            assertEquals(TestWalOperation.Put(bytes("key"), bytes("value")), entries[0].operation)
            assertEquals(TestWalOperation.Delete(bytes("other")), entries[1].operation)
        }
    }

    @Test
    fun `WAL ignores a torn final entry and continues its sequence`() = withTempDirectory { directory ->
        val walPath = directory.resolve("store.wal")

        FileWriteAheadLog(walPath, walOperationCodec()).use { wal ->
            wal.append(TestWalOperation.Put(bytes("first"), bytes("kept")))
            wal.append(TestWalOperation.Put(bytes("second"), bytes("torn")))
            wal.sync()
        }

        FileChannel.open(walPath, WRITE).use { channel ->
            channel.truncate(channel.size() - 1L)
        }

        FileWriteAheadLog(walPath, walOperationCodec()).use { wal ->
            val entries = wal.replay().toList()
            assertEquals(1, entries.size)
            assertEquals(1L, entries.single().lsn)
            assertEquals(2L, wal.append(TestWalOperation.Delete(bytes("first"))))
            wal.sync()
        }

        FileWriteAheadLog(walPath, walOperationCodec()).use { wal ->
            val entries = wal.replay().toList()
            assertEquals(listOf(1L, 2L), entries.map { it.lsn })
            assertEquals(TestWalOperation.Delete(bytes("first")), entries.last().operation)
        }
    }

    @Test
    fun `store restores latest values and deletions after reopening`() = withTempDirectory { directory ->
        val walPath = directory.resolve("store.wal")

        openStringStore(walPath).use { firstStore ->
            firstStore.put("kept", bytes("old"))
            firstStore.put("kept", bytes("latest"))
            firstStore.put("deleted", bytes("value"))
            firstStore.delete("deleted")
        }

        openStringStore(walPath).use { recoveredStore ->
            assertContentEquals(bytes("latest"), recoveredStore.get("kept"))
            assertNull(recoveredStore.get("deleted"))
        }
    }

    private fun openStringStore(walPath: Path): LwwMapWithWal<String> = LwwMapWithWal(
        wal = FileWriteAheadLog(
            walPath,
            LwwWalOperationCodec(
                encodeKey = { it.toByteArray(Charsets.UTF_8) },
                decodeKey = { it.toString(Charsets.UTF_8) },
            ),
        ),
        replicaId = 1L,
    )

    private fun bytes(value: String): ByteArray = value.toByteArray(Charsets.UTF_8)

    private fun walOperationCodec() = object : WalOperationCodec<TestWalOperation> {
        override fun encode(operation: TestWalOperation): ByteArray {
            val bytes = ByteArrayOutputStream()
            DataOutputStream(bytes).use { output ->
                when (operation) {
                    is TestWalOperation.Put -> {
                        output.writeByte(1)
                        writeBytes(output, operation.key)
                        writeBytes(output, operation.value)
                    }
                    is TestWalOperation.Delete -> {
                        output.writeByte(2)
                        writeBytes(output, operation.key)
                    }
                }
            }
            return bytes.toByteArray()
        }

        override fun decode(bytes: ByteArray): TestWalOperation {
            DataInputStream(ByteArrayInputStream(bytes)).use { input ->
                val operation = when (input.readUnsignedByte()) {
                    1 -> TestWalOperation.Put(readBytes(input), readBytes(input))
                    2 -> TestWalOperation.Delete(readBytes(input))
                    else -> error("Unknown test WAL operation")
                }
                check(input.available() == 0) { "Unexpected trailing bytes" }
                return operation
            }
        }

        private fun writeBytes(output: DataOutputStream, bytes: ByteArray) {
            output.writeInt(bytes.size)
            output.write(bytes)
        }

        private fun readBytes(input: DataInputStream): ByteArray {
            val size = input.readInt()
            require(size >= 0 && size <= input.available()) { "Invalid byte array size" }
            return ByteArray(size).also(input::readFully)
        }
    }

    private fun stringStateCodec() = MapBinaryCodec<String, LwwRecord<ByteArray>>(
        encodeKey = { it.toByteArray(Charsets.UTF_8) },
        decodeKey = { it.toString(Charsets.UTF_8) },
        itemCodec = LwwRecordCodec,
    )

    private fun assertStatesEqual(
        expected: LwwMapState<String, ByteArray>,
        actual: LwwMapState<String, ByteArray>,
    ) {
        assertEquals(expected.entries.size, actual.entries.size)
        expected.entries.forEach { (expectedKey, expectedRecord) ->
            val actualRecord = actual.entries[expectedKey]
                ?: fail("Missing key $expectedKey")

            assertEquals(expectedRecord.version, actualRecord.version)
            when (expectedRecord) {
                is LwwRecord.Value -> {
                    val actualValue = actualRecord as? LwwRecord.Value
                        ?: fail("Record type mismatch for $expectedKey")
                    assertContentEquals(expectedRecord.value, actualValue.value)
                }

                is LwwRecord.Tombstone -> {
                    if (actualRecord !is LwwRecord.Tombstone) {
                        fail("Record type mismatch for $expectedKey")
                    }
                }
            }
        }
    }

    private fun withTempDirectory(block: (Path) -> Unit) {
        val directory = Files.createTempDirectory("kv-storage-smoke")
        try {
            block(directory)
        } finally {
            Files.walk(directory).use { paths ->
                paths.sorted(Comparator.reverseOrder()).forEach { Files.deleteIfExists(it) }
            }
        }
    }
}
