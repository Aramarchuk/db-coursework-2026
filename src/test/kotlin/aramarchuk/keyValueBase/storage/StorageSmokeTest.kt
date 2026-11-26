package aramarchuk.keyValueBase.storage

import java.nio.channels.FileChannel
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption.WRITE
import java.util.Comparator
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertNull

class StorageSmokeTest {
    @Test
    fun `WAL replays operations after reopening`() = withTempDirectory { directory ->
        val walPath = directory.resolve("store.wal")

        FileWriteAheadLog(walPath).use { wal ->
            assertEquals(1L, wal.append(WalOperation.Put(bytes("key"), bytes("value"))))
            assertEquals(2L, wal.append(WalOperation.Delete(bytes("other"))))
            wal.sync()
        }

        FileWriteAheadLog(walPath).use { wal ->
            val entries = wal.replay().toList()
            assertEquals(listOf(1L, 2L), entries.map { it.lsn })
            assertEquals(WalOperation.Put(bytes("key"), bytes("value")), entries[0].operation)
            assertEquals(WalOperation.Delete(bytes("other")), entries[1].operation)
        }
    }

    @Test
    fun `WAL ignores a torn final entry and continues its sequence`() = withTempDirectory { directory ->
        val walPath = directory.resolve("store.wal")

        FileWriteAheadLog(walPath).use { wal ->
            wal.append(WalOperation.Put(bytes("first"), bytes("kept")))
            wal.append(WalOperation.Put(bytes("second"), bytes("torn")))
            wal.sync()
        }

        FileChannel.open(walPath, WRITE).use { channel ->
            channel.truncate(channel.size() - 1L)
        }

        FileWriteAheadLog(walPath).use { wal ->
            val entries = wal.replay().toList()
            assertEquals(1, entries.size)
            assertEquals(1L, entries.single().lsn)
            assertEquals(2L, wal.append(WalOperation.Delete(bytes("first"))))
            wal.sync()
        }

        FileWriteAheadLog(walPath).use { wal ->
            val entries = wal.replay().toList()
            assertEquals(listOf(1L, 2L), entries.map { it.lsn })
            assertEquals(WalOperation.Delete(bytes("first")), entries.last().operation)
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

    private fun openStringStore(walPath: Path): WalBackedKVStore<String> = WalBackedKVStore(
        wal = FileWriteAheadLog(walPath),
        encodeKey = { it.toByteArray(Charsets.UTF_8) },
        decodeKey = { it.toString(Charsets.UTF_8) },
        replicaId = 1L,
    )

    private fun bytes(value: String): ByteArray = value.toByteArray(Charsets.UTF_8)

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
