package aramarchuk.keyValueBase.storage

import java.nio.file.Files
import java.nio.file.Path
import java.util.Comparator
import kotlin.test.Test
import kotlin.test.assertEquals

class CounterMapWithWalTest {
    @Test
    fun `counter operations survive WAL recovery`() = withTempDirectory { directory ->
        val walPath = directory.resolve("counter.wal")

        openCounterMap(walPath, replicaId = 12L).use { counters ->
            counters.increment("apples")
            counters.increment("apples")
            counters.decrement("apples")
            assertEquals(1L, counters.get("apples"))
        }

        openCounterMap(walPath, replicaId = 12L).use { recovered ->
            assertEquals(1L, recovered.get("apples"))
            recovered.decrement("apples")
            assertEquals(0L, recovered.get("apples"))
        }

        openCounterMap(walPath, replicaId = 12L).use { recoveredAgain ->
            assertEquals(0L, recoveredAgain.get("apples"))
        }
    }

    @Test
    fun `replicas converge after merge and preserve merged state on restart`() =
        withTempDirectory { directory ->
            val leftWal = directory.resolve("left.wal")
            val rightWal = directory.resolve("right.wal")

            openCounterMap(leftWal, replicaId = 2L).use { left ->
                openCounterMap(rightWal, replicaId = 17L).use { right ->
                    left.increment("visits")
                    left.increment("visits")
                    left.decrement("visits")

                    right.increment("visits")
                    right.decrement("visits")
                    right.decrement("visits")
                    right.decrement("visits")

                    val leftBeforeMerge = left.snapshot()
                    val rightBeforeMerge = right.snapshot()
                    left.merge(rightBeforeMerge)
                    right.merge(leftBeforeMerge)

                    assertEquals(-1L, left.get("visits"))
                    assertEquals(-1L, right.get("visits"))
                    assertEquals(left.snapshot(), right.snapshot())

                    // Re-delivery must not count the peer's increments twice.
                    val converged = left.snapshot()
                    left.merge(right.snapshot())
                    right.merge(converged)
                    assertEquals(converged, left.snapshot())
                    assertEquals(converged, right.snapshot())
                }
            }

            openCounterMap(leftWal, replicaId = 2L).use { leftRecovered ->
                openCounterMap(rightWal, replicaId = 17L).use { rightRecovered ->
                    assertEquals(-1L, leftRecovered.get("visits"))
                    assertEquals(-1L, rightRecovered.get("visits"))
                    assertEquals(leftRecovered.snapshot(), rightRecovered.snapshot())
                }
            }
        }

    private fun openCounterMap(path: Path, replicaId: Long): CounterMapWithWal<String> =
        CounterMapWithWal(
            replicaId = replicaId,
            wal = FileWriteAheadLog(
                path,
                CounterOperationCodec(
                    encodeKey = { it.toByteArray(Charsets.UTF_8) },
                    decodeKey = { it.toString(Charsets.UTF_8) },
                ),
            ),
        )

    private fun withTempDirectory(block: (Path) -> Unit) {
        val directory = Files.createTempDirectory("counter-map-wal-test")
        try {
            block(directory)
        } finally {
            Files.walk(directory).use { paths ->
                paths.sorted(Comparator.reverseOrder()).forEach { Files.deleteIfExists(it) }
            }
        }
    }
}
