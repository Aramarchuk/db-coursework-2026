package aramarchuk.keyValueBase

import aramarchuk.keyValueBase.storage.FileWriteAheadLog
import aramarchuk.keyValueBase.storage.KVStore
import aramarchuk.keyValueBase.storage.WalBackedKVStore
import java.nio.file.Path

fun main() {
    val store: KVStore<String> = WalBackedKVStore(
        wal = FileWriteAheadLog(Path.of("data", "store.wal")),
        encodeKey = { it.toByteArray(Charsets.UTF_8) },
        decodeKey = { it.toString(Charsets.UTF_8) },
        replicaId = 1L,
    )

    println("Persistent key-value store (data directory: ./data). Commands: put <key> <value>, get <key>, delete <key>, exit")

    try {
        while (true) {
            print("> ")
            System.out.flush()

            val line = readlnOrNull() ?: break
            if (line.isBlank()) continue

            val parts = line.trim().split(Regex("\\s+"), limit = 3)
            when (parts[0].lowercase()) {
                "put" -> {
                    if (parts.size < 3) {
                        println("Usage: put <key> <value>")
                    } else {
                        store.put(parts[1], parts[2].toByteArray(Charsets.UTF_8))
                        println("OK")
                    }
                }

                "get" -> {
                    if (parts.size != 2) {
                        println("Usage: get <key>")
                    } else {
                        val value = store.get(parts[1])
                        println(value?.toString(Charsets.UTF_8) ?: "(not found)")
                    }
                }

                "delete" -> {
                    if (parts.size != 2) {
                        println("Usage: delete <key>")
                    } else {
                        store.delete(parts[1])
                        println("OK")
                    }
                }

                "exit", "quit" -> break
                else -> println("Unknown command: ${parts[0]}")
            }
        }
    } finally {
        store.close()
    }
}
